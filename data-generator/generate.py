import random, sys, time
import psycopg2

DSN = "host=localhost port=5433 dbname=shop user=shop password=shop123"
CITIES = ["Manama", "Riffa", "Muharraq", "Dubai", "Riyadh", "Doha", "Muscat", "Kuwait City"]
STATUSES = ["NEW", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
CATEGORIES = ["electronics", "books", "home", "toys", "sports"]
FIRST = ["Ali", "Sara", "Omar", "Noor", "Hassan", "Maryam", "Yusuf", "Layla", "Khalid", "Fatima"]
LAST = ["Ahmed", "Hussain", "Salman", "Jaber", "Mansoor", "Khalifa", "Yacoub", "Darwish"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
  customer_id SERIAL PRIMARY KEY,
  full_name   TEXT NOT NULL,
  email       TEXT,
  city        TEXT,
  created_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  category   TEXT NOT NULL,
  price      NUMERIC(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
  order_id    SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id),
  product_id  INT NOT NULL REFERENCES products(product_id),
  quantity    INT NOT NULL,
  amount      NUMERIC(10,2) NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT now(),
  updated_at  TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;
"""

def connect():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn

def new_customer(cur, bad=False):
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    email = None if bad else f"{name.lower().replace(' ', '.')}{random.randint(1, 9999)}@example.com"
    cur.execute("INSERT INTO customers (full_name, email, city) VALUES (%s,%s,%s)",
                (name, email, random.choice(CITIES)))

def new_order(cur, bad=False):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    cust = cur.fetchone()[0]
    cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT 1")
    prod, price = cur.fetchone()
    qty = random.randint(1, 5)
    amount = -abs(price * qty) if bad else price * qty
    cur.execute("INSERT INTO orders (customer_id, product_id, quantity, amount, status) VALUES (%s,%s,%s,%s,'NEW')",
                (cust, prod, qty, amount))

def seed(cur):
    cur.execute(SCHEMA)
    cur.execute("SELECT count(*) FROM products")
    if cur.fetchone()[0] > 0:
        print("Already seeded."); return
    for i in range(10):
        cur.execute("INSERT INTO products (name, category, price) VALUES (%s,%s,%s)",
                    (f"Product {i+1}", random.choice(CATEGORIES), round(random.uniform(3, 400), 2)))
    for i in range(30):
        new_customer(cur, bad=(i % 10 == 0))
    for i in range(60):
        new_order(cur, bad=(i % 20 == 0))
    print("Seeded 10 products, 30 customers, 60 orders.")

def stream(cur, seconds):
    end = time.time() + seconds
    while time.time() < end:
        r = random.random()
        if r < 0.6:
            new_order(cur, bad=(random.random() < 0.03))
        elif r < 0.9:
            cur.execute("""UPDATE orders SET status = %s, updated_at = now()
                           WHERE order_id = (SELECT order_id FROM orders WHERE status NOT IN ('DELIVERED','CANCELLED')
                                             ORDER BY random() LIMIT 1)""", (random.choice(STATUSES[1:]),))
        elif r < 0.97:
            new_customer(cur)
        else:
            cur.execute("UPDATE customers SET city = %s WHERE customer_id = (SELECT customer_id FROM customers ORDER BY random() LIMIT 1)",
                        (random.choice(CITIES),))
        time.sleep(0.5)
    print("Done streaming.")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "seed"
    conn = connect(); cur = conn.cursor()
    if mode == "seed":
        seed(cur)
    elif mode == "stream":
        stream(cur, int(sys.argv[2]) if len(sys.argv) > 2 else 120)