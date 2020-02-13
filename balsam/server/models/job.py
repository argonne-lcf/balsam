from django.db import models, transaction
from django.db.models import Count, Q, F
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from datetime import timedelta
import logging
from .exceptions import InvalidStateError, ValidationError
from .transfer import TransferItem

logger = logging.getLogger(__name__)

ALLOWED_TRANSITIONS = {
    "CREATED": ("AWAITING_PARENTS", "READY",),
    "AWAITING_PARENTS": ("READY",),
    "READY": ("STAGED_IN", "FAILED",),
    "STAGED_IN": ("PREPROCESSED", "FAILED",),
    "PREPROCESSED": ("RUNNING",),
    "RUNNING": ("RUN_DONE", "RUN_ERROR", "RUN_TIMEOUT", "KILLED",),
    "RUN_DONE": ("POSTPROCESSED", "RESTART_READY", "FAILED",),
    "POSTPROCESSED": ("JOB_FINISHED", "FAILED",),
    "JOB_FINISHED": ("RESET",),
    "RUN_ERROR": ("POSTPROCESSED", "RESTART_READY", "FAILED",),
    "RUN_TIMEOUT": ("POSTPROCESSED", "RESTART_READY", "FAILED",),
    "RESTART_READY": ("RUNNING",),
    "FAILED": ("RESET",),
    "KILLED": ("RESET",),
    "RESET": ("AWAITING_PARENTS", "READY",),
}

# Lock signifies something is working on the job
LOCKED_STATUS = {
    "READY": "Staging in",
    "STAGED_IN": "Preprocessing",
    "PREPROCESSED": "Acquired by launcher",
    "RESTART_READY": "Acquired by launcher",
    "RUNNING": "Running",
    "RUN_DONE": "Postprocessing",
    "RUN_ERROR": "Postprocessing (Error handling)",
    "RUN_TIMEOUT": "Postprocessing (Timeout handling)",
    "POSTPROCESSED": "Staging out",
}

STATE_CHOICES = [(k, k.capitalize().replace("_", " ")) for k in ALLOWED_TRANSITIONS]


class EventLogManager(models.Manager):
    def log_transition(
        self, job, old_state, new_state, timestamp=None, message="", save=True
    ):
        if timestamp is None:
            timestamp = timezone.now()
        log = self.model(
            job=job,
            from_state=old_state,
            to_state=new_state,
            timestamp=timestamp,
            message=message,
        )
        if save:
            log.save()
        return log

    def log_update(self, job, message):
        log = self.model(
            job=job,
            from_state=job.state,
            to_state=job.state,
            timestamp=timezone.now(),
            message=message,
        )
        log.save()
        return log


class EventLog(models.Model):
    objects = EventLogManager()
    job = models.ForeignKey("Job", related_name="events", on_delete=models.CASCADE,)
    from_state = models.CharField(max_length=64)
    to_state = models.CharField(max_length=64)
    timestamp = models.DateTimeField(auto_now=False)
    message = models.TextField()


class JobLockManager(models.Manager):
    EXPIRATION_PERIOD = timedelta(minutes=3)

    def create(self, site, label, batch_job):
        lock = JobLock(site=site, label=label, batch_job=batch_job)
        lock.save()
        return lock

    def clear_stale(self):
        expiry_time = timezone.now() - self.EXPIRATION_PERIOD
        qs = self.get_queryset()
        expired_locks = qs.filter(heartbeat__lte=expiry_time)
        num_deleted, _ = expired_locks.delete()
        logger.info(f"Cleared {num_deleted} expired locks")


class JobLock(models.Model):
    objects = JobLockManager()
    heartbeat = models.DateTimeField(auto_now=True)
    label = models.CharField(blank=True, default="", max_length=64)
    site = models.ForeignKey("Site", on_delete=models.CASCADE)
    batch_job = models.ForeignKey(
        "BatchJob", blank=True, null=True, default=None, on_delete=models.SET_NULL
    )

    def tick(self):
        self.save(update_fields=["heartbeat"])

    def release(self):
        logger.info(f"Released lock {self.pk}")
        self.delete()


class JobManager(models.Manager):
    def chain_prefetch_for_update(self, qs):
        """Prefetch related items that would be hit in a big list view"""
        return qs.select_related(
            "lock",
            "app_exchange",
            "app_backend",
            "app_backend__site",
            "batch_job",
            "parents",
        ).prefetch_related("app_exchange__backends", "transfer_items", "events")

    @transaction.atomic
    def bulk_create(self, job_list):
        jobs = []
        for dat in job_list:
            job = self.create(**dat)
            jobs.append(job)
        return jobs

    @transaction.atomic
    def bulk_update(self, patch_list):
        patch_map = {}
        for patch in patch_list:
            pk = patch.pop("pk")
            patch_map[pk] = patch

        qs = self.get_queryset().filter(pk__in=patch_map)
        qs = self.chain_prefetch_for_update(qs)
        for job in qs.order_by("pk").select_for_update():
            patch = patch_map[job.pk]
            job.update(**patch)
        return qs

    @transaction.atomic
    def bulk_delete_queryset(self, queryset):
        for job in queryset.select_related("lock"):
            job.delete()

    def create(
        self,
        workdir,
        tags,
        owner,
        app_exchange,
        transfer_items,
        parameters,
        data,
        parents,
        num_nodes,
        ranks_per_node,
        threads_per_rank,
        threads_per_core,
        cpu_affinity,
        gpus_per_rank,
        node_packing_count,
        wall_time_min,
        **kwargs,
    ):
        job = self.model(
            workdir=workdir,
            tags=tags,
            owner=owner,
            app_exchange=app_exchange,
            app_backend=None,
            parameters=parameters,
            data=data,
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            cpu_affinity=cpu_affinity,
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
        )
        job.save()
        job.parents.set(parents)
        for dat in transfer_items:
            TransferItem.objects.create(
                direction=dat["direction"],
                source=dat["source"],
                destination=dat["destination"],
                job=job,
            )

        job.reset_backend()
        if job.is_waiting_for_parents():
            job.update_state("AWAITING_PARENTS")
        else:
            job.update_state("READY")
        return job

    def _lock_for_acquire(self, queryset):
        """
        Hit the DB to acquire the lock (order by pk to avoid deadlocks)
        Return a new Queryset limited to this set of locked pks
        """
        # Using filter(lock=None).select_for_update(skip_locked=False)
        # should do the right thing and serialize access to pre-selected rows
        # skip_locked=True would improve concurrency only if a small LIMIT was used
        locked_pks = list(
            queryset.order_by("pk").select_for_update().values_list("pk", flat=True)
        )
        return self.get_queryset().filter(pk__in=locked_pks)

    @transaction.atomic
    def acquire(
        self,
        lock,
        acquire_unbound,
        states,
        max_num_acquire,
        filter_tags=None,
        node_resources=None,
        order_by=(),
    ):
        site = lock.site
        already_bound = Q(app_backend__site=site)
        eligible_to_bind = Q(app_exchange__backends__site=site)

        # if acquire_unbound, sort jobs by bound-first, but allow unbound
        if acquire_unbound:
            qs = self.get_queryset().filter(eligible_to_bind | already_bound)
            order_by = [F("app_backend").desc(nulls_last=True)] + list(order_by)
        else:
            qs = self.get_queryset().filter(already_bound)
            order_by = list(order_by)

        if filter_tags is None:
            filter_tags = {}

        qs = (
            qs.filter(lock__isnull=True)
            .filter(state__in=states)
            .filter(tags__contains=filter_tags)  # Job.keys contains all k:v pairs
        )

        if node_resources is None:
            qs = self._lock_for_acquire(qs).order_by(*order_by)[:max_num_acquire]
            return self._lock_and_bind(qs, lock)
        else:
            qs = self._lock_for_acquire(qs).order_by(*order_by)
            qs = self._pre_assign(qs, node_resources, max_num_acquire)
            return self._lock_and_bind(qs, lock)

    def _lock_and_bind(self, queryset, lock):
        """
        Sets app_backend, lock, and batch_job on the selected queryset
        Returns the queryset of newly acquired jobs
        """
        for job in queryset:
            job.lock = lock
            if lock.batch_job_id is not None:
                job.batch_job_id = lock.batch_job_id
            if job.app_backend is None:
                backend = job.app_exchange.backends.get(site=lock.site)
                job.app_backend = backend
            job.save()
        return queryset

    def _pre_assign(self, ordered_qs, node_resources, max_num_acquire):
        """
        Iterate `ordered_qs` (a la launcher) to pre-assign jobs for
        execution, according to the launcher client's `node_resources` constraints.
        A reasonable ordering is: (-num_nodes, -wall_time_minutes, node_packing_count)
        No more than `max_num_acquire` jobs will be selected.
        """
        max_wall_time_min = node_resources.pop("max_wall_time_min")
        ordered_qs = ordered_qs.filter(wall_time_min__lte=max_wall_time_min)

        resources = NodeResources(**node_resources)
        max_nodes = resources.num_available_nodes()
        ordered_qs = ordered_qs.filter(num_nodes__lte=max_nodes)
        placed_jobs = []

        for job in ordered_qs:
            assigned_nodes = resources.attempt_assign(job)
            if assigned_nodes:
                job.assigned_nodes = assigned_nodes
                placed_jobs.append(job)

                if len(placed_jobs) >= max_num_acquire:
                    break
                if resources.num_available_nodes() < 1:
                    break

        return placed_jobs


class NodeResources(object):
    def __init__(
        self,
        max_jobs_per_node,
        running_job_counts,
        node_occupancies,
        idle_cores,
        idle_gpus,
    ):
        self.max_jobs_per_node = max_jobs_per_node
        self.running_job_counts = running_job_counts
        self.node_occupancies = node_occupancies
        self.idle_cores = idle_cores
        self.idle_gpus = idle_gpus
        self.num_nodes = len(self.running_job_counts)
        assert (
            self.num_nodes == len(idle_cores) == len(idle_gpus) == len(node_occupancies)
        )

    def num_available_nodes(self):
        return sum(
            bool(node_job_count < self.max_jobs_per_node)
            for node_job_count in self.running_job_counts
        )

    def attempt_assign(self, job):
        required_num_nodes = int(job.num_nodes)
        num_cores = job.ranks_per_node * job.threads_per_rank / job.threads_per_core
        num_gpus = job.ranks_per_node * job.gpus_per_rank
        occ = 1.0 / job.node_packing_count

        eligible_nodes = []
        for node_idx in range(self.num_nodes):
            if self.running_job_counts[node_idx] >= self.max_jobs_per_node:
                continue
            if self.idle_cores[node_idx] < num_cores:
                continue
            if self.idle_gpus[node_idx] < num_gpus:
                continue
            if occ + self.node_occupancies[node_idx] > 1.0001:
                continue
            eligible_nodes.append(node_idx)
            if len(eligible_nodes) == required_num_nodes:
                self._do_assign(self, eligible_nodes, num_cores, num_gpus, occ)
                return eligible_nodes
        return []

    def _do_assign(self, node_indices, num_cores, num_gpus, occ):
        for idx in node_indices:
            self.running_job_counts[idx] += 1
            self.idle_cores[idx] -= num_cores
            self.idle_gpus[idx] -= num_gpus
            self.node_occupancies[idx] += occ


class Job(models.Model):
    objects = JobManager()

    # Metadata
    workdir = models.CharField(
        """Workdir *relative* to site data directory (cannot start with '/')""",
        max_length=256,
    )
    tags = JSONField(
        """
        Use like K8s selectors.
        A shallow dict of k:v string pairs
        Replace "workflow_filter"
        But also used for all CRUD operations
        # -t formula=H2O -t method__startswith=CC""",
        default=dict,
    )
    lock = models.ForeignKey(
        "JobLock",
        null=True,
        blank=True,
        default=None,
        on_delete=models.SET_NULL,
        db_index=True,
        editable=False,
    )
    owner = models.ForeignKey(
        "User",
        null=False,
        editable=False,
        on_delete=models.CASCADE,
        related_name="jobs",
    )

    app_exchange = models.ForeignKey(
        "AppExchange", related_name="jobs", on_delete=models.CASCADE
    )
    app_backend = models.ForeignKey(
        "AppBackend",
        on_delete=models.CASCADE,
        null=True,
        editable=False,
        related_name="jobs",
    )
    parameters = JSONField(default=dict)

    batch_job = models.ForeignKey(
        "BatchJob",
        on_delete=models.SET_NULL,
        editable=False,
        related_name="balsam_jobs",
        null=True,
        blank=True,
    )
    state = models.CharField(
        max_length=32,
        default="CREATED",
        editable=False,
        db_index=True,
        choices=STATE_CHOICES,
    )
    last_update = models.DateTimeField(auto_now=True)
    data = JSONField(default=dict)
    return_code = models.IntegerField(blank=True, null=True)
    last_error = models.TextField(blank=True, default="")

    # DAG: each Job can refer to 'parents' and 'children' attrs
    parents = models.ManyToManyField(
        "self",
        verbose_name="Parent Jobs",
        blank=True,
        symmetrical=False,
        editable=False,
        related_name="children",
    )

    # Resource Specification
    num_nodes = models.IntegerField(default=1)
    ranks_per_node = models.IntegerField(default=1)
    threads_per_rank = models.IntegerField(default=1)
    threads_per_core = models.IntegerField(default=1)
    cpu_affinity = models.CharField(max_length=32, default="depth")
    gpus_per_rank = models.IntegerField(default=0)
    node_packing_count = models.IntegerField(default=1)
    wall_time_min = models.IntegerField(default=0)

    # Not Directly Updateable fields:
    # parents, app_exchange, owner: set at creation
    # app_backend: set in reset_backend() or acquire()
    # lock: set by acquire(); unset in update_state()
    # batchjob: set in acquire() when from a launcher context
    def update(
        self,
        tags=None,
        workdir=None,
        parameters=None,
        state=None,
        state_timestamp=None,
        state_message="",
        return_code=None,
        data=None,
        transfer_items=None,
        num_nodes=None,
        ranks_per_node=None,
        threads_per_rank=None,
        threads_per_core=None,
        cpu_affinity=None,
        gpus_per_rank=None,
        node_packing_count=None,
        wall_time_min=None,
    ):
        update_kwargs = dict(
            tags=tags,
            workdir=workdir,
            parameters=parameters,
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            cpu_affinity=cpu_affinity,
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
        )
        update_kwargs = {k: v for k, v in update_kwargs.items() if v is not None}

        if return_code is not None:
            self.return_code = return_code

        if data is not None:
            self.data.update(data)
            EventLog.objects.log_update(self, f"Set data {data.keys()}")

        self.update_transfer_status(transfer_items)

        if not self.is_locked():
            for kwarg, new_value in update_kwargs.items():
                old_value = getattr(self, kwarg)
                setattr(self, kwarg, new_value)
                EventLog.objects.log_update(
                    self, f"{kwarg.capitalize()} changed: {old_value} -> {new_value}"
                )

        if state is not None:
            self.update_state(state, message=state_message, timestamp=state_timestamp)
        else:
            self.save()

    def update_transfer_status(self, transfer_status_updates):
        if transfer_status_updates is None:
            return

        pk_map = {transfer.pk: transfer for transfer in self.transfer_items.all()}
        for update in transfer_status_updates:
            pk = update.pop("pk")
            transfer_item = pk_map[pk]
            transfer_item.update(**update)

    def delete(self, *args, **kwargs):
        if self.is_locked():
            status = LOCKED_STATUS.get(self.state)
            msg = f"Can't delete active Job {self.pk}: currently {status}"
            raise ValidationError(msg)
        super().delete(*args, **kwargs)

    def update_state(self, new_state, message="", timestamp=None):
        if new_state not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidStateError(
                f"Cannot transition from {self.state} to {new_state}"
            )

        old_state = self.state
        self.state = new_state

        if new_state == "RUN_ERROR":
            self.last_error = message
        elif new_state == "FAILED" and message:
            self.last_error = message

        if self.is_locked() and new_state != "RUNNING":
            self.release_lock()

        self.save()
        EventLog.objects.log_transition(
            job=self,
            old_state=old_state,
            new_state=new_state,
            timestamp=timestamp,
            message=message,
            save=True,
        )
        transition_func = getattr(self, f"on_{new_state}", None)
        if transition_func:
            transition_func(timestamp=timestamp)

    def on_READY(self, *args, timestamp=None, **kwargs):
        # If the Job is not bound to a site, leave it READY
        if self.app_backend is None:
            return
        # Otherwise, a Job with only one execution site and nothing
        # to transfer in can skip ahead
        if self.transfer_items.filter(direction="in").count() == 0:
            self.update_state(
                "STAGED_IN", message="Skipped stage-in", timestamp=timestamp
            )

    def on_RESET(self, *args, timestamp=None, **kwargs):
        self.reset_backend()
        if self.is_waiting_for_parents():
            self.update_state(
                "AWAITING_PARENTS",
                message="Waiting on at least one parent to finish",
                timestamp=timestamp,
            )
        else:
            self.update_state("READY", timestamp=timestamp)

    def on_POSTPROCESSED(self, *args, timestamp=None, **kwargs):
        if self.transfer_items.filter(direction="out").count() == 0:
            self.update_state("JOB_FINISHED", "Skipped stage-out", timestamp)

    @transaction.atomic
    def on_JOB_FINISHED(self, *args, timestamp=None, **kwargs):
        qs = self.children.order_by("pk").select_for_update()
        qs = qs.annotate(num_parents=Count("parents", distinct=True))
        qs = qs.annotate(
            finished_parents=Count(
                "parents", distinct=True, filter=Q(parents__state="JOB_FINISHED")
            )
        )
        for child in qs:
            if child.num_parents == child.finished_parents:
                child.update_state(
                    "READY", message="All parents finished", timestamp=timestamp
                )

    def is_waiting_for_parents(self):
        parent_count = self.parents.count()
        if parent_count == 0:
            return False
        return self.parents.filter(state="JOB_FINISHED").count() < parent_count

    def reset_backend(self):
        # If App has only one backend; bind it automatically
        backends = list(self.app_exchange.backends.all())
        if len(backends) == 1:
            self.app_backend = backends[0]
        else:
            self.app_backend = None

    def is_locked(self):
        return self.lock is not None

    def release_lock(self):
        self.lock = None
