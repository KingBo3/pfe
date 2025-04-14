import os
import logging
from datetime import datetime, date
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient
from dotenv import load_dotenv

# Chargement des variables d'environnement depuis un fichier .env (si nécessaire)
# Pensez à créer un fichier .env et y stocker, par exemple :
# POSTGRES_HOST=localhost
# POSTGRES_DB=pfe
# POSTGRES_USER=postgres
# POSTGRES_PASSWORD=123123
# POSTGRES_PORT=5432
# MONGO_URI=mongodb://localhost:27017/
# MONGO_DBNAME=pfetest
# MONGO_COLLECTION=reservation_rooms_data1

load_dotenv()

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    """
    Établit et retourne une connexion à la base PostgreSQL
    en utilisant les variables d'environnement.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            database=os.getenv('POSTGRES_DB', 'pfe'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', '123123'),
            port=os.getenv('POSTGRES_PORT', 5432)
        )
        logger.info("Connexion à PostgreSQL établie avec succès.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Erreur de connexion à PostgreSQL: {e}")
        raise

def get_mongo_client():
    """
    Établit et retourne un client MongoDB
    en utilisant les variables d'environnement.
    """
    try:
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        client = MongoClient(mongo_uri)
        logger.info("Connexion à MongoDB établie avec succès.")
        return client
    except Exception as e:
        logger.error(f"Erreur de connexion à MongoDB: {e}")
        raise

def extract_data_from_postgres(conn):
    """
    Extrait les données depuis PostgreSQL à l'aide d'une requête SQL.
    Retourne une liste de dictionnaires.
    """
    query = sql.SQL("""
        SELECT
            rr.reservation_id,
            rr.room_type_id AS requested_room_type_id,
            rrt.code AS requested_room_type_code,
            art.code AS assigned_room_type_code,
            rr.arrival AS arrival_date,
            rr.departure AS departure_date,
            rr.stay,
            rr.booking_date,
            gc.country AS origin_city,
            ro.name AS origin_reservation,
            rs.name AS source_reservation,
            rr.number_of_adult + rr.number_of_child + rr.inf AS occupancy,
            rr.card_group_id,
            agency.name AS card_group_name,
            rr.card_group_type,
            rr.market_code_id,
            mc.name AS market_code_name,
            rt.code AS rate_code,
            rt.name AS rate_name,

            /* Sous-requête pour le nombre de réservations annulées */
            (
                SELECT COUNT(*)
                FROM magic_hotels_skanes.reservation_rooms r2
                JOIN magic_hotels_skanes.reservation_states s2 ON s2.id = r2.reservation_state_id
                WHERE r2.master_guest_id = rr.master_guest_id
                AND s2.system_value = 'cancelled'
            ) AS preview_cancelled,

            false AS preview_no_show,

            (
                SELECT rrs.created_at
                FROM magic_hotels_skanes.reservation_room_states rrs
                JOIN magic_hotels_skanes.reservation_states st ON st.id = rrs.reservation_state_id
                WHERE rrs.reservation_room_id = rr.id
                AND st.system_value = 'cancelled'
                ORDER BY rrs.created_at ASC
                LIMIT 1
            ) AS cancelled_date,

            NULL AS no_show_date,
            NULL AS preference_guest,
            NULL AS preference_reservation,
            NULL AS season

        FROM magic_hotels_skanes.reservation_rooms rr
        LEFT JOIN magic_hotels_skanes.room_types rrt ON rr.room_type_id = rrt.id
        LEFT JOIN magic_hotels.guest_cards gc ON rr.master_guest_id = gc.id
        LEFT JOIN magic_hotels_skanes.room_types art ON rr.assigned_room_type_id = art.id
        LEFT JOIN magic_hotels_skanes.reservation_origins ro ON rr.reservation_origin_id = ro.id
        LEFT JOIN magic_hotels_skanes.reservation_sources rs ON rr.reservation_source_id = rs.id
        LEFT JOIN magic_hotels.market_codes mc ON rr.market_code_id = mc.id
        LEFT JOIN magic_hotels_skanes.reservation_room_nights rrn ON rrn.reservation_room_id = rr.id
        LEFT JOIN magic_hotels_skanes.rates rt ON rrn.rate_id = rt.id
        LEFT JOIN magic_hotels.agency_cards agency ON rr.card_group_id = agency.id
     

    """)

    data = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            for row in rows:
                record = dict(zip(colnames, row))
                data.append(record)
        logger.info(f"{len(data)} enregistrements extraits de PostgreSQL.")
    except psycopg2.Error as e:
        logger.error(f"Erreur lors de l'extraction des données: {e}")
        raise

    return data

def transform_data(records):
    """
    Transforme les données avant l'insertion dans MongoDB.
    Par exemple, conversion des dates en chaînes de caractères ISO 8601.
    """
    for record in records:
        for key, value in record.items():
            if isinstance(value, (datetime, date)):
                record[key] = value.isoformat()
    return records

def load_data_to_mongo(client, records):
    """
    Insère les données dans la collection MongoDB spécifiée.
    """
    db_name = os.getenv('MONGO_DBNAME', 'pfetest')
    collection_name = os.getenv('MONGO_COLLECTION', 'reservation_rooms_data1')

    db = client[db_name]
    collection = db[collection_name]

    if not records:
        logger.info("Aucun document à insérer dans MongoDB.")
        return

    try:
        result = collection.insert_many(records)
        logger.info(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans MongoDB: {e}")
        raise

def main():
    pg_conn = None
    mongo_client = None

    try:
        pg_conn = get_postgres_connection()
        data = extract_data_from_postgres(pg_conn)

        # Transformations éventuelles
        data = transform_data(data)

        mongo_client = get_mongo_client()
        load_data_to_mongo(mongo_client, data)

    except Exception as e:
        logger.error(f"Erreur dans le script principal: {e}")
    finally:
        # Fermeture des connexions
        if pg_conn:
            pg_conn.close()
            logger.info("Connexion à PostgreSQL fermée.")
        if mongo_client:
            mongo_client.close()
            logger.info("Connexion à MongoDB fermée.")

if __name__ == "__main__":
    main()
