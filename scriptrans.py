import psycopg2
from pymongo import MongoClient
from datetime import datetime, date, time


# PostgreSQL connection configuration
pg_config = {
    'host': 'localhost',
    'database': 'pfe',
    'user': 'postgres',
    'password': '123123',
    'port': 5432
}

# MongoDB configuration
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "pfetest"
mongo_collection_name = "reservation_rooms_data1"

# Fields to transfer (for reference only)
target_fields = [
    'reservation_id', 'requested_room_type_id', 'requested_room_type_code', 'assigned_room_type_code',
    'arrival_date', 'departure_date', 'stay', 'booking_date',
    'origin_city', 'origin_reservation', 'source_reservation',
    'occupancy', 'card_group_id', 'card_group_name', 'card_group_type',
    'market_code_id', 'market_code_name',
    'rate_code', 'rate_name',
    'preview_cancelled', 'preview_no_show',
    'cancelled_date', 'no_show_date',
    'preference_guest', 'preference_reservation', 'season'
]

try:
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**pg_config)
    pg_cursor = pg_conn.cursor()

    # Connect to MongoDB
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    # SQL Query - join necessary tables
    query = """
    SELECT
        rr.reservation_id,
        rr.room_type_id AS requested_room_type_id,
        rrt.code AS requested_room_type_code,
        art.code AS assigned_room_type_code,
        rr.arrival AS arrival_date,
        rr.departure AS departure_date,
        rr.stay,
        rr.booking_date,
        '' AS origin_city,
        ro.name AS origin_reservation,
        rs.name AS source_reservation,
        rr.number_of_adult + rr.number_of_child + rr.inf AS occupancy,
        rr.card_group_id,
        '' AS card_group_name,
        rr.card_group_type,
        rr.market_code_id,
        mc.name AS market_code_name,
        rt.code AS rate_code,
        rt.name AS rate_name,
        false AS preview_cancelled,
        false AS preview_no_show,
        rr.canceled_at AS cancelled_date,
        NULL AS no_show_date,
        NULL AS preference_guest,
        NULL AS preference_reservation,
        NULL AS season
    FROM
        magic_hotels_skanes.reservation_rooms rr
    LEFT JOIN magic_hotels_skanes.room_types rrt ON rr.room_type_id = rrt.id
    LEFT JOIN magic_hotels_skanes.room_types art ON rr.assigned_room_type_id = art.id
    LEFT JOIN magic_hotels_skanes.reservation_origins ro ON rr.reservation_origin_id = ro.id
    LEFT JOIN magic_hotels_skanes.reservation_sources rs ON rr.reservation_source_id = rs.id
    LEFT JOIN magic_hotels.market_codes mc ON rr.market_code_id = mc.id
    LEFT JOIN magic_hotels_skanes.reservation_room_nights rrn ON rrn.reservation_room_id = rr.id
    LEFT JOIN magic_hotels_skanes.rates rt ON rrn.rate_id = rt.id
    """

    pg_cursor.execute(query)
    rows = pg_cursor.fetchall()
    colnames = [desc[0] for desc in pg_cursor.description]

    # Prepare and insert into MongoDB
    documents = []
    for row in rows:
        doc = dict(zip(colnames, row))
        # Convert datetime.date to string if needed
        for k, v in doc.items():
            if isinstance(v, (datetime, date)):
                doc[k] = v.isoformat()
        documents.append(doc)

    if documents:
        mongo_collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into MongoDB.")

except Exception as e:
    print("Error occurred:", e)

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    if 'mongo_client' in locals():
        mongo_client.close()
