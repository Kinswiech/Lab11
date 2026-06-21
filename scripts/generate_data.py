import os
import time
from datetime import datetime, timedelta

os.makedirs("data/input_stream", exist_ok=True)

batches = [
    [
        ["2026-05-01 10:00:00", "u001", "books", 39.99, "paid"],
        ["2026-05-01 10:02:00", "u002", "electronics", 299.99, "paid"],
        ["2026-05-01 10:04:00", "u003", "books", 19.99, "cancelled"],
    ],
    [
        ["2026-05-01 10:06:00", "u004", "clothes", 89.99, "paid"],
        ["2026-05-01 10:08:00", "u005", "books", 49.99, "paid"],
        ["2026-05-01 10:09:00", "u006", "electronics", 599.99, "paid"],
    ],
    [
        ["2026-05-01 10:12:00", "u007", "books", 29.99, "paid"],
        ["2026-05-01 10:15:00", "u008", "clothes", 120.00, "paid"],
        ["2026-05-01 09:50:00", "u009", "books", 9.99, "paid"],
    ],
]

header = "event_time,user_id,category,amount,status\n"

for i, batch in enumerate(batches, start=1):
    file_path = f"data/input_stream/events_{i}.csv"

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(header)
        for row in batch:
            file.write(",".join(map(str, row)) + "\n")

    print(f"Utworzono plik: {file_path}")
    time.sleep(8)