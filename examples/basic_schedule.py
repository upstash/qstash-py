"""
Create a schedule that publishes a message every minute.
"""

from upstash_qstash import QStash


def main():
    qstash = QStash(
        token="<QSTASH-TOKEN>",
    )

    schedule_id = qstash.schedule.create_json(
        cron="* * * * *",
        destination="https://example..com",
        body={"hello": "world"},
    )

    # Print out the schedule ID
    print(schedule_id)

    # You can also get a schedule by ID
    schedule = qstash.schedule.get(schedule_id)
    print(schedule.cron)


if __name__ == "__main__":
    main()
