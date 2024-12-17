schema = {
    "listen_events": [
        {"name": "artist", "type": "STRING", "mode": "NULLABLE"},
        {"name": "song", "type": "STRING", "mode": "NULLABLE"},
        {"name": "duration", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "sessionId", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "auth", "type": "STRING", "mode": "NULLABLE"},
        {"name": "level", "type": "STRING", "mode": "NULLABLE"},
        {"name": "itemInSession", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "zip", "type": "STRING", "mode": "NULLABLE"},  # Added
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lon", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"}
    ],
    "page_view_events": [
        {"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "sessionId", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "page", "type": "STRING", "mode": "NULLABLE"},
        {"name": "auth", "type": "STRING", "mode": "NULLABLE"},
        {"name": "method", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "level", "type": "STRING", "mode": "NULLABLE"},
        {"name": "itemInSession", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "zip", "type": "STRING", "mode": "NULLABLE"},  # Added
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lon", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "artist", "type": "STRING", "mode": "NULLABLE"},
        {"name": "song", "type": "STRING", "mode": "NULLABLE"},
        {"name": "duration", "type": "FLOAT64", "mode": "NULLABLE"}
    ],
    "auth_events": [
        {"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "sessionId", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "level", "type": "STRING", "mode": "NULLABLE"},
        {"name": "itemInSession", "type": "INTEGER", "mode": "NULLABLE"},  # Added
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "zip", "type": "STRING", "mode": "NULLABLE"},  # Added
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lon", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "success", "type": "BOOLEAN", "mode": "NULLABLE"}
    ]
}
