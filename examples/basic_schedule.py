"""
Create a schedule that publishes a message every minute.
"""

from qstash import QStash


def main() -> None:
    client = QStash(
        token="<QSTASH-TOKEN>",
    )

    schedule_id = client.schedule.create_json(
        cron="* * * * *",
        destination="https://example..com",
        body={"hello": "world"},
    )

    # Print out the schedule ID
    print(schedule_id)

    # You can also get a schedule by ID
    schedule = client.schedule.get(schedule_id)
    print(schedule.cron)


if __name__ == "__main__":
    main()
