from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys


def connect_to_cassandra(contact_points=None):
    """
    Connects to the Cassandra server and returns a session object.
    """
    if contact_points is None:
        contact_points = ['cassandra-server']  # Default Docker service name

    print("Connecting to Cassandra...")
    try:
        cluster = Cluster(contact_points)
        session = cluster.connect()
        print("Connected successfully!")
        return session
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        sys.exit(1)


def display_keyspaces(session):
    """
    Displays all available keyspaces in the Cassandra database.
    """
    print("Fetching available keyspaces...")
    try:
        rows = session.execute('DESCRIBE keyspaces')
        print("Available keyspaces:")
        for row in rows:
            print(row.keyspace_name)
    except Exception as e:
        print(f"Error fetching keyspaces: {e}")


def create_keyspace_and_tables(session):
    """
    Creates a keyspace and required tables in Cassandra for storing index data.
    """
    print("Creating keyspace and tables...")
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS index_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        session.set_keyspace('index_keyspace')

        session.execute("""
            CREATE TABLE IF NOT EXISTS vocabulary (
                term text PRIMARY KEY,
                doc_ids list<text>
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS document_index (
                term text,
                doc_id text,
                term_frequency int,
                PRIMARY KEY (term, doc_id)
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS bm25_stats (
                term text PRIMARY KEY,
                document_frequency int,
                total_term_frequency int
            );
        """)
        print("Keyspace and tables created successfully!")
    except Exception as e:
        print(f"Error creating keyspace or tables: {e}")


def main():
    """
    Main function to connect to Cassandra, display keyspaces, and create schema.
    """
    print("Starting Cassandra setup...")
    
    # Step 1: Connect to Cassandra
    session = connect_to_cassandra()

    # Step 2: Display available keyspaces
    display_keyspaces(session)

    # Step 3: Create keyspace and tables
    create_keyspace_and_tables(session)

    print("Cassandra setup completed!")


if __name__ == "__main__":
    main()