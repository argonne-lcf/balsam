import logging

from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm.exc import NoResultFound

from balsam.server import settings
from balsam.util import config_root_logger

from .auth import auth, user_from_token
from .pubsub import pubsub
from .routers import apps, batch_jobs, events, jobs, sessions, sites, transfers

app = FastAPI(
    title="Balsam API",
    version="0.1.0",
)


def setup_logging():
    _, handler = config_root_logger(settings.balsam_log_level)
    sqa_logger = logging.getLogger("sqlalchemy")
    sqa_logger.setLevel(settings.sqlalchemy_log_level)
    sqa_logger.handlers.clear()
    sqa_logger.addHandler(handler)
    logging.getLogger("sqlalchemy.engine").propagate = True
    return logging.getLogger("balsam.server.main")


logger = setup_logging()


@app.exception_handler(NoResultFound)
async def no_result_handler(request: Request, exc: NoResultFound):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "Not found"},
    )


app.include_router(
    auth.router,
    prefix="/users",
    tags=["users"],
    dependencies=[],
)

app.include_router(
    sites.router,
    prefix="/sites",
    tags=["sites"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    apps.router,
    prefix="/apps",
    tags=["apps"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    jobs.router,
    prefix="/jobs",
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    events.router,
    prefix="/events",
    tags=["events"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    batch_jobs.router,
    prefix="/batch-jobs",
    tags=["batch-jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    transfers.router,
    prefix="/transfers",
    tags=["transfers"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    sessions.router,
    prefix="/sessions",
    tags=["sessions"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)


@app.websocket("/subscribe-user")
async def subscribe_user(websocket: WebSocket):
    # Accept and receive token
    await websocket.accept()
    token = await websocket.receive_text()
    try:
        user = user_from_token(token)
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    p = await pubsub.subscribe(user.id)
    while True:
        msg = await p.get_message(timeout=1.0)
        if msg and msg["type"] == "message":
            await websocket.send_bytes(msg["data"])


ws_html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Balsam Websocket</title>
    </head>
    <body>
        <h1>Balsam websocket log</h1>
        <ul id='messages'>
        </ul>
        <script>
            let form = new FormData()
            let reader = new FileReader()
            form.set("username", "misha")
            form.set("password", "foo")
            fetch(
                "http://localhost:8000/users/login", {
                    method: 'POST',
                    body: form
                }
            )
            .then(response => response.json())
            .then(data => {
                console.log("Using token to auth websocket", data.access_token)
                let ws = new WebSocket("ws://localhost:8000/subscribe-user");
                ws.onopen = function(event) {
                    ws.send(data.access_token);
                }
                ws.onclose = function(event) {
                    var messages = document.getElementById('messages')
                    var message = document.createElement('li')
                    message.textContent = "The websocket was closed."
                    messages.appendChild(message)
                }
                ws.onmessage = async function(event) {
                    var messages = document.getElementById('messages')
                    var message = document.createElement('li')
                    if (event.data instanceof Blob) {
                        message.textContent = await event.data.text()
                    }
                    else {
                        message.textContent = event.data
                    }
                    messages.appendChild(message)
                };
            })
        </script>
    </body>
</html>
"""


@app.get("/user-events")
def get_user_events():
    return HTMLResponse(ws_html)


logger.info(f"Loaded balsam.server.main\n{settings}")


# 1) Parameter in path
# @app.get("/items/{item_id}")
# async def read_item(item_id: int):
# Or explicitly declare with fastapi.Path()
#   first kwarg should always be ellipsis (to say no default value)
#   kwargs: title, description, numeric validators: ge, gt, lt, le

# Use Enums for choice-only parameters

# 2) Query params (singular types; NOT in path)
# Can be required, have default value, or set Optional[type] = None
# To accept a list use Query object:
#       q: List[str] = Query(None)
# To set other validators on the Query:
# q: str = Query(None, min_length=3, max_length=50)
# Query parameter can have metadata
#   Query(None, title="", description="", min_length="", deprecated=True)

# 3) Request body params: Pydantic model types
# Multiple body parameters: parameter names used as top-level keys
# Or use fastapi.Body(...) to declare a singular value as request Body
# Use List[Job] to declare that top-level of JSON body is a list

# Pydantic metadata
# set default values in pydantic models to pydantic.Field() instance
# First arg is the default (use ellipsis if required; None if not)
# title, max_length, min_length, gt, ge, lt, le, all available...

# Add subclass "Config" to Pydantic model with "schema_extra" attr
# "schema_extra" is a dict showing an example of the instance

# 4) Read parameters from Cookies with fastapi.Cookie
# token: str = Cookie(None)

# 5) Read parameters from request Header with fastapi.Header
# user_agent: str = Header(None)
# Pydantic will automatically convert header keys to snake_case

# Control how output is serialized:
# @app.post("/jobs/", response_model=JobOut)
# response_model can also be a list of models
# Use response_model_exclude_unset=True if needed

# If necessary, use fastapi.encoders.jsonable_encoder

# Set tags=["jobs"] to group related paths

# Set default success status code:
# @app.post("/jobs/" status_code=status.HTTP_201_CREATED)
# Or raise HTTPException(status=code, detail="reason")

# Docstring determines description; can use markdown

# Partial update example
#  @app.patch("/items/{item_id}", response_model=Item)
#  async def update_item(item_id: str, item: Item):
#      stored_item_data = items[item_id]
#      stored_item_model = Item(**stored_item_data)
#      update_data = item.dict(exclude_unset=True)
#      updated_item = stored_item_model.copy(update=update_data)
#      items[item_id] = jsonable_encoder(updated_item)
#      return updated_item
