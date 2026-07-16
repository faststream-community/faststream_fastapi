# Background tasks

You can use background tasks in faststream handlers.

```python
from fastapi import BackgroundTasks

async def task() -> None: ...

@broker.subscriber("subject")
async def handle(tasks: BackgroundTasks) -> None:
    tasks.add_task(task)
```
