import sqlite3
import mysql.connector
import re
import sys

def sqlite_type_to_mysql(sqlite_type):
    t = sqlite_type.upper()
    if 'INT' in t:
        return 'INT'
    elif 'CHAR' in t or 'CLOB' in t or 'TEXT' in t:
        return 'TEXT'
    elif 'BLOB' in t:
        return 'BLOB'
    elif 'REAL' in t or 'FLOA' in t or 'DOUB' in t:
        return 'DOUBLE'
    elif 'DATE' in t or 'TIME' in t:
        return 'DATETIME'
    else:
        return 'TEXT'

def parse_columns(schema):
    cols = re.search(r'\((.*)\)', schema, re.S)
    if not cols:
        return []
    col_defs = []
    # Split on commas that are not inside parentheses
    parts = re.split(r',\s*(?![^()]*\))', cols.group(1))
    for line in parts:
        line = line.strip()
        # Skip constraints/keys (handled later if needed)
        if not line or line.upper().startswith(('CONSTRAINT', 'PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')):
            continue
        col_match = re.match(r'[`"\[]?(\w+)[`"\]]?\s+([^\s,]+)', line)
        if col_match:
            col_name, col_type = col_match.groups()
            mysql_type = sqlite_type_to_mysql(col_type)
            col_defs.append((col_name, mysql_type))
    return col_defs

def get_tables(conn):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cursor if not row[0].startswith('sqlite_')]

def get_indexes(conn, table):
    index_list = []
    cursor = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql NOT NULL", (table,))
    for name, sql in cursor.fetchall():
        # Parse index type (UNIQUE or not), columns
        unique = 'UNIQUE' in sql.upper()
        m = re.search(r'\(([^)]+)\)', sql)
        if m:
            columns = [col.strip().strip('`"[]') for col in m.group(1).split(',')]
            index_list.append({'name': name, 'unique': unique, 'columns': columns})
    return index_list

def main(sqlite_file, host, user, password, database, batch_size=100):
    # Connect to SQLite
    conn_sqlite = sqlite3.connect(sqlite_file)
    cursor_sqlite = conn_sqlite.cursor()

    # Connect to MariaDB/MySQL
    conn_mysql = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor_mysql = conn_mysql.cursor()

    tables = get_tables(conn_sqlite)
    print(f"Tables found: {tables}")

    for table in tables:
        # Table schema and columns
        cursor_sqlite.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,))
        row = cursor_sqlite.fetchone()
        if row and row[0]:
            schema = row[0]
            columns = parse_columns(schema)
            print(f"Columns for {table}: {[col for col, typ in columns]}")
            col_defs = ", ".join([f"`{col}` {typ}" for col, typ in columns])
            create_sql = f"CREATE TABLE IF NOT EXISTS `{table}` ({col_defs});"
            print(f"Creating table `{table}`...")
            cursor_mysql.execute(f"DROP TABLE IF EXISTS `{table}`;")
            cursor_mysql.execute(create_sql)
            conn_mysql.commit()

            # Indexes
            indexes = get_indexes(conn_sqlite, table)
            for idx in indexes:
                idx_cols = ', '.join(f"`{c}`" for c in idx['columns'])
                if idx['unique']:
                    idx_sql = f"CREATE UNIQUE INDEX `{idx['name']}` ON `{table}` ({idx_cols});"
                else:
                    idx_sql = f"CREATE INDEX `{idx['name']}` ON `{table}` ({idx_cols});"
                print(f"Creating index on `{table}`: {idx_sql}")
                try:
                    cursor_mysql.execute(idx_sql)
                except Exception as e:
                    print(f"Warning: Could not create index `{idx['name']}`: {e}")

            # Data migration in batches
            cursor_sqlite.execute(f"SELECT * FROM `{table}`;")
            rows = cursor_sqlite.fetchall()
            if rows:
                cols = [desc[0] for desc in cursor_sqlite.description]
                print(f"SQLite columns for {table}: {cols}")
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    values_str = ', '.join(['(' + ', '.join(['%s']*len(cols)) + ')' for _ in batch])
                    insert_sql = f"INSERT INTO `{table}` ({', '.join('`'+c+'`' for c in cols)}) VALUES {values_str}"
                    values = [item for row in batch for item in row]
                    try:
                        cursor_mysql.execute(insert_sql, values)
                        conn_mysql.commit()
                    except Exception as e:
                        print(f"Error inserting batch {i//batch_size + 1} for table `{table}`: {e}")
                    print(f"Inserted batch {i//batch_size + 1} of table `{table}`")
            else:
                print(f"No data for table `{table}`.")

    cursor_mysql.close()
    conn_mysql.close()
    cursor_sqlite.close()
    conn_sqlite.close()
    print("Migration complete.")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Migrate SQLite database to MariaDB/MySQL with indexes.")
    parser.add_argument("sqlite_file", help="Path to SQLite .db file")
    parser.add_argument("mariadb_host", help="MariaDB host")
    parser.add_argument("mariadb_user", help="MariaDB user")
    parser.add_argument("mariadb_password", help="MariaDB password")
    parser.add_argument("mariadb_database", help="MariaDB database name")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for multi-row inserts (default: 100)")
    args = parser.parse_args()

    main(
        args.sqlite_file,
        args.mariadb_host,
        args.mariadb_user,
        args.mariadb_password,
        args.mariadb_database,
        args.batch_size
    )
