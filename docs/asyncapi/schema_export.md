# How to Generate and Serve AsyncAPI Documentation

In this guide, let's explore how to generate and serve [AsyncAPI](https://www.asyncapi.com/) documentation for our FastStream application.


## Writing the FastStream Application

Here's an example Python application using **FastStream** that consumes data from a topic,
increments the value, and outputs the data to another topic. Save it in a file called `basic.py`

```python title="basic.py"
from pydantic import BaseModel, Field, NonNegativeFloat

from fastapi import FastAPI
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI
from faststream_fastapi import FastStreamAPI, Logger

class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ...,
        examples=[0.5],
        description="Float data example",
    )

broker = NatsBroker("localhost:9092")
application = FastStreamAPI(
    broker,
    application=FastAPI(),
    specification=AsyncAPI(),
)

@broker.publisher("output")
@broker.subscriber("input")
async def on_input_data(msg: DataBasic, logger: Logger) -> DataBasic:
    logger.info(msg)
    return DataBasic(data=msg.data + 1.0)
```

## Generating the AsyncAPI Specification#

Now that we have a FastStream application and specification object,
we can proceed with generating the AsyncAPI specification using a CLI command.

```bash
faststream docs gen basic:application
```

The above command will generate the AsyncAPI specification and save it in a file called `asyncapi.json`.

If you prefer `yaml` instead of `json`, please run the following command to generate `asyncapi.yaml`.

```bash
faststream docs gen --yaml basic:application
```
!!! Tip

    To generate the documentation in yaml format, please install the necessary dependency to work with YAML file format at first.

    ```bash
    pip install PyYAML
    ```
