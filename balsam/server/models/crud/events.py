from balsam.server import models


def fetch(db, owner, paginator, filterset):
    qs = db.query(models.LogEvent).join(models.Job).join(models.App).join(models.Site)
    qs = qs.filter(models.Site.owner_id == owner.id)
    qs = filterset.apply_filters(qs)
    count = qs.group_by(models.LogEvent.id).count()
    events = paginator.paginate(qs)
    print(*((e.from_state, e.to_state) for e in events), sep="\n")
    return count, events
