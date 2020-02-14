from rest_framework import status


class SiteFactoryMixin:
    def create_site(
        self, hostname="baz", path="/foo", check=status.HTTP_201_CREATED, client=None
    ):
        if client is None:
            client = self.client
        return client.post_data("site-list", hostname=hostname, path=path, check=check)


class AppFactoryMixin:
    def create_app(
        self,
        name="hello world",
        sites=None,
        cls_names=None,
        parameters=["name", "N"],
        check=status.HTTP_201_CREATED,
        client=None,
    ):
        """Sites: dict with pk, or list of dicts with pk, or list of ints, or int"""
        if client is None:
            client = self.client
        # Site validation
        if isinstance(sites, dict):
            sites = [int(sites["pk"])]
        elif isinstance(sites, list):
            sites = [(int(s["pk"]) if isinstance(s, dict) else int(s)) for s in sites]
        elif isinstance(sites, int):
            sites = [sites]
        else:
            raise ValueError("sites must be a single-or-list of ints or dicts-with-pk")
        # Class Names validation
        if isinstance(cls_names, str):
            cls_names = [cls_names]
        elif isinstance(cls_names, list):
            cls_names = [str(s) for s in cls_names]
        else:
            raise ValueError("cls_names")

        backends = [
            {"site": pk, "class_name": name} for pk, name in zip(sites, cls_names)
        ]
        if parameters is None:
            parameters = ["name", "N"]
        return client.post_data(
            "app-list",
            check=check,
            name=name,
            backends=backends,
            parameters=parameters,
        )


class BatchJobFactoryMixin:
    def create_batchjob(
        self,
        site,
        project="datascience",
        queue="default",
        num_nodes=128,
        wall_time_min=30,
        job_mode="mpi",
        filter_tags={"system": "H2O", "type": "sp_energy"},
        check=status.HTTP_201_CREATED,
        client=None,
    ):
        if client is None:
            client = self.client
        return client.post_data(
            "batchjob-list",
            site=site["pk"],
            project=project,
            queue=queue,
            num_nodes=num_nodes,
            wall_time_min=wall_time_min,
            job_mode=job_mode,
            filter_tags=filter_tags,
            check=check,
        )


class JobFactoryMixin:
    def job_dict(
        self,
        workdir="test/1",
        tags={},
        app=None,
        transfers=[],
        parameters={"name": "world", "N": 4},
        data={},
        parents=[],
        num_nodes=2,
        ranks_per_node=4,
        threads_per_rank=1,
        threads_per_core=1,
        cpu_affinity="depth",
        gpus_per_rank=0,
        node_packing_count=1,
        wall_time_min=0,
    ):
        if app is None:
            app = self.default_app
        if isinstance(app, dict):
            app = app["pk"]
        return dict(
            workdir=workdir,
            tags=tags,
            app=app,
            transfer_items=transfers,
            parameters=parameters,
            data=data,
            parents=parents,
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            cpu_affinity=cpu_affinity,
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
        )

    def create_jobs(self, new_jobs, client=None, check=status.HTTP_201_CREATED):
        if client is None:
            client = self.client
        if isinstance(new_jobs, dict):
            new_jobs = [new_jobs]
            return client.bulk_post_data("job-list", new_jobs, check=check)[0]
        return client.bulk_post_data("job-list", new_jobs, check=check)

    def create_session(self, site, label="", batch_job=None, client=None):
        if client is None:
            client = self.client

        return client.post_data(
            "session-list",
            site=site["pk"],
            label=label,
            batch_job=batch_job,
            check=status.HTTP_201_CREATED,
        )

    def acquire_jobs(
        self,
        session,
        acquire_unbound,
        states,
        max_num_acquire,
        filter_tags={},
        node_resources=None,
        order_by=None,
        client=None,
        check=status.HTTP_200_OK,
    ):
        if client is None:
            client = self.client

        response = client.post_data(
            "session-detail",
            uri={"pk": session["pk"]},
            acquire_unbound=acquire_unbound,
            states=states,
            max_num_acquire=max_num_acquire,
            filter_tags=filter_tags,
            node_resources=node_resources,
            order_by=order_by,
            check=check,
        )
        if check == status.HTTP_200_OK:
            return response["acquired_jobs"]
        return None
