import requests
import sqlite3
import os
import time
import random


def check_database_exists():
    """
    Create the database transactions.db if it does not exist.
    Here the UNIQUE constraint will maintain we don't insert duplicate
    payment_id's into the table in that database
    """
    if not os.path.exists('../data/transactions.db'):
        conn = sqlite3.connect('../data/transactions.db')
        c = conn.cursor()
        c.execute("""CREATE TABLE transactions
                         (payment_id, actor, created_time,
                         message, comments, permalink, story_id,
                         transactions, transaction_type, updated_time, 
                         UNIQUE(payment_id))""")
        conn.commit()
        conn.close()


def get_vens(num=1000):
    """Make a single request to the Public API endpoint for lots of payments...Kind of scary this data is public"""
    url = f"https://venmo.com/api/v5/public?limit={num}"
    try:
        vens = requests.get(url).json()['data']
    except Exception as e:
        print(f"Error getting transactions: {e}")
        return None
    return vens


def insert_vens(vens, cursor):
    """Insert or replace transactions denoted by payment_id into database where payment_id is the primary unique key"""
    while vens:
        try:
            d = dict(vens.pop())
            payment_id, actor, created_time = d['payment_id'], d['actor'], d['created_time']
            message, comments = d['message'], d['comments']
            mentions, permalink, story_id = d['mentions'], d['permalink'], d['story_id']
            transactions, transaction_type, updated_time = d['transactions'], d['type'], \
                                                           d['updated_time']
            print_ven_to_console(payment_id, actor, transactions, message, cursor)
            cursor.execute(
                'INSERT OR REPLACE INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?)', (payment_id, str(actor),
                                                                                     created_time, message,
                                                                                     str(comments), permalink,
                                                                                     story_id,
                                                                                     str(transactions),
                                                                                     transaction_type,
                                                                                     updated_time))
        except Exception as e:
            print(f"Exception inserting to database {e}")


def print_ven_to_console(payment_id, actor, transactions, message, cursor):
    """Print transaction w/ Emoji to console.. Unix & Mac should work for sure"""
    try:
        cursor.execute("SELECT * FROM transactions WHERE payment_id = ?", (payment_id,))
        data = cursor.fetchone()
        if not data:
            print(f"{actor['username']} ----> {transactions[0]['target']['username']}\n{message}\n")
        time.sleep(.01)
    except (TypeError, KeyError) as e:
        print(f"Exception: {e}")


def stream_and_insert(num, cursor):
    """Continuous stream of Venmo payments to your a sqlite database & your terminal"""
    while True:
        try:
            vens = get_vens(num)
            if vens:
                insert_vens(vens, cursor=cursor)
            # Sleeping for random integer value between 10 and 20, doing some type of
            # background fetching & consuming from a queue might work better
            time.sleep(random.randint(10, 20))

        except KeyboardInterrupt:
            print('\n\n***Breaking loop and saving database***\n')
            cursor.execute("SELECT COUNT (*) FROM transactions")
            rowcount = cursor.fetchone()[0]
            print(f"\nNumber of comments: {rowcount}\n")
            break


def main():
    check_database_exists()
    conn = sqlite3.connect("../data/transactions.db")
    c = conn.cursor()
    stream_and_insert(100000000, cursor=c)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
