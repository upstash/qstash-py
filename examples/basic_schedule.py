"""
Create a schedule that publishes a message every minute if there are no 
existing schedules.

Also an example of how to use the schedules API (list, create, delete, get).
"""

from qstash_tokens import QSTASH_TOKEN
from upstash_qstash import Client


def main():
    client = Client(QSTASH_TOKEN)
    schedules = client.schedules()

    if len(schedules.list()) > 0:
        print("There are already schedules. Exiting.")
        return

    res = schedules.create(
        {
            "cron": "* * * * *",
            "destination": "https://py-qstash-testing.requestcatcher.com",
            "body": {"hello": "world"},
            "headers": {
                "content-type": "application/json",  # This is the default, but you can override it
            },
        }
    )

    # Print out the schedule ID
    sched_id = res["scheduleId"]
    print(sched_id)

    # You can also get a schedule by ID
    sched = schedules.get(sched_id)
    print(sched["cron"])


def delete_all_schedules():
    client = Client(QSTASH_TOKEN)
    schedules = client.schedules()
    for sched in schedules.list():
        schedules.delete(sched["scheduleId"])


if __name__ == "__main__":
    main()
