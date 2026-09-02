import asyncio
from datetime import UTC, datetime
from random import uniform
from uuid import uuid4

import httpx
from dotenv import load_dotenv

load_dotenv()


async def main() -> None:
    sequence_no = 0
    async with httpx.AsyncClient() as client:
        while True:
            sequence_no += 1
            event = {
                "eventId": str(uuid4()), "deviceId": "sensor-204", "tenantId": "acme",
                "timestamp": datetime.now(UTC).isoformat(), "sequenceNo": sequence_no,
                "metric": "temperature_c", "value": round(uniform(20, 30), 2),
            }
            response = await client.post("http://127.0.0.1:8000/v1/telemetry", json=event)
            print(response.status_code, event)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
