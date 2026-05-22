# Tutorial – migrating the `leafy_popup_store` retail database

This tutorial walks through a complete `mongo2pg` workflow using the
[`leafy_popup_store`](https://github.com/mongodb-developer/leafy_popup_store)
sample retail dataset from MongoDB Developer.

The database contains seven collections that represent a small e-commerce
application: `carts`, `invoices`, `locations`, `orders`, `products`,
`recommendations`, and `users`.

---

## Prerequisites

- `mongo2pg` installed (`cargo install --path .` from the repo root, or a
  pre-built binary — see [USAGE.md](../USAGE.md))
- MongoDB running locally (the examples below use port **2717**)
- [`mongorestore`](https://www.mongodb.com/docs/database-tools/mongorestore/)
  from the MongoDB Database Tools

---

## Step 1 – Download the dataset

Clone or download the BSON dump from the
[leafy_popup_store repository](https://github.com/mongodb-developer/leafy_popup_store):

```bash
git clone https://github.com/mongodb-developer/leafy_popup_store.git
# The BSON dumps are in dump/ or a similar sub-directory inside the repo.
# Place (or copy) the *.bson.gz files into a local folder, e.g.
#   results/leafy_popup_store/
```

---

## Step 2 – Import into MongoDB

```bash
cd /path/to/mongodb-database-tools/bin

for f in /path/to/leafy_popup_store/*.bson.gz; do
  collection=$(basename "$f" .bson.gz)
  ./mongorestore --gzip --drop \
    --uri="mongodb://user:pass@localhost:2717" \
    --authenticationDatabase admin \
    --db retail \
    --collection "$collection" \
    "$f"
done
```

This imports every collection into a database called `retail`.  Adjust the
URI, credentials, and path to the `.bson.gz` files as needed.

---

## Step 3 – Create the migration project

```bash
mongo2pg init \
  --project-base results/base \
  --project-name retail \
  --uri "mongodb://user:pass@localhost:2717/?authSource=admin"
```

`init` creates the following directory tree and writes a config file:

```
results/base/retail/
    config/retail.conf          ← BASE_DIR, PROJECT_DIR, URI, NAMESPACE
    schema/tables/              ← SQL DDL files (populated by to-pg)
    source/collections/         ← inferred schemas + stats (populated by infer)
    data/
    reports/                    ← HTML reports (populated by report)
```

Then open `results/base/retail/config/retail.conf` and add the `NAMESPACE`
line so that `infer` knows which database to analyse:

```
BASE_DIR  = results/base
PROJECT_DIR = retail
URI       = mongodb://user:pass@localhost:2717/?authSource=admin
NAMESPACE = retail
```

---

## Step 4 – Infer the schemas

```bash
mongo2pg infer -c results/base/retail/config/retail.conf
```

For each collection `mongo2pg` samples documents, infers a probabilistic
schema, prints stats to stderr, and writes three files per collection:

```
source/collections/<name>/
    <name>.json          ← full inferred schema (JSON)
    <name>.stats.txt     ← human-readable statistics
    <name>.stats.yaml    ← structured statistics (used by report)
```

### Example stats output

**`products`** (760 documents, very flat):

```
Documents in collection : 760
Documents sampled       : 760
Width (top-level / max)  : 14 / 14 (L1)
Depth (max nesting)     : 2
Branch (per level)      : L1:14  L2:4
Top-level types         : _id:ObjectId, articleType:String, baseColour:String,
                          brand:String, code:String, description:String, gender:String,
                          image:Object, masterCategory:String, name:String,
                          price:Object, subCategory:String, vai_text_embedding:Array,
                          year:Number
```

**`orders`** (4 documents, nested arrays):

```
Documents in collection : 4
Documents sampled       : 4
Width (top-level / max)  : 7 / 10 (L3)
Depth (max nesting)     : 4
Branch (per level)      : L1:7  L2:2  L3:10  L4:3
Top-level types         : _id:ObjectId, invoiceId:ObjectId, products:Array,
                          shipping_address:String, status_history:Array,
                          type:String, user:ObjectId
```

**`locations`** (6 documents, fully flat):

```
Documents in collection : 6
Documents sampled       : 6
Width (top-level / max)  : 8 / 8 (L1)
Depth (max nesting)     : 1
Branch (per level)      : L1:8
Top-level types         : _id:ObjectId, city:String, country:String, cp:String,
                          name:String, state:String, street_and_number:String, type:String
```

**`users`**:

```
Documents in collection : 4
Documents sampled       : 4
Width (top-level / max)  : 8 / 8 (L1)
Depth (max nesting)     : 3
Branch (per level)      : L1:8  L2:6  L3:6
Top-level types         : _id:ObjectId, address:Object, email:String,
                          lastRecommendations:Array, name:String, surname:String,
                          type:String, version:Number
```

---

## Step 5 – Generate PostgreSQL DDL

```bash
mongo2pg to-pg -c results/base/retail/config/retail.conf
```

Each `.json` schema is read from `source/collections/<name>/<name>.json` and
a `.sql` file is written to `schema/tables/<name>.sql`.

`mongo2pg` flattens nested objects and arrays into child tables with foreign
keys.

### Example – `products.sql`

```sql
CREATE TABLE products (
    id UUID PRIMARY KEY,
    articletype TEXT NOT NULL,
    basecolour TEXT,
    brand TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL,
    gender TEXT NOT NULL,
    mastercategory TEXT,
    name TEXT NOT NULL,
    subcategory TEXT,
    year INTEGER
);

CREATE TABLE products_image (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id)
);

CREATE TABLE products_price (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id)
);

CREATE TABLE products_vai_text_embedding (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id)
);
```

### Example – `orders.sql`

The nested `products` array and `status_history` array each become a child
table; the doubly-nested `image` and `price` objects become grandchild tables:

```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    invoiceid TEXT NOT NULL,
    shipping_address TEXT NOT NULL,
    type TEXT NOT NULL,
    _user TEXT NOT NULL
);

CREATE TABLE orders_products (
    id BIGSERIAL PRIMARY KEY,
    orders_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    brand TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (orders_id) REFERENCES orders (id)
);

CREATE TABLE orders_status_history (
    id BIGSERIAL PRIMARY KEY,
    orders_id UUID NOT NULL,
    status TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    FOREIGN KEY (orders_id) REFERENCES orders (id)
);

CREATE TABLE orders_products_image (
    id BIGSERIAL PRIMARY KEY,
    orders_products_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (orders_products_id) REFERENCES orders_products (id)
);

CREATE TABLE orders_products_price (
    id BIGSERIAL PRIMARY KEY,
    orders_products_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT,
    FOREIGN KEY (orders_products_id) REFERENCES orders_products (id)
);
```

---

## Step 6 – Generate reports

```bash
mongo2pg report -c results/base/retail/config/retail.conf
```

Two HTML files are written to `reports/`:

| File | Contents |
|---|---|
| `retail.html` | Collection stats table (documents, depth, width, field counts) |
| `retail.schema.html` | Entity-relationship diagram built from the generated SQL files |

Open them in any browser to explore the database structure.

---

## Step 7 – Export data to gzipped CSV

```bash
mongo2pg export -c results/base/retail/config/retail.conf
```

`export` streams every document from MongoDB, expands nested arrays and objects
into child tables (following the same hierarchy that `to-pg` generated), and
writes one `.csv.gz` file per SQL table into `data/<collection_name>/`.

### Output layout

```
data/
├── carts/
│   ├── carts.csv.gz
│   └── carts_products.csv.gz
├── invoices/
│   ├── invoices.csv.gz
│   ├── invoices_items.csv.gz
│   ├── invoices_items_image.csv.gz
│   ├── invoices_items_price.csv.gz
│   ├── invoices_metadata.csv.gz
│   ├── invoices_metadata_creditcardprocessing.csv.gz
│   ├── invoices_metadata_erpdetails.csv.gz
│   ├── invoices_metadata_frauddetection.csv.gz
│   ├── invoices_metadata_loyaltyrewards.csv.gz
│   └── invoices_recommendations.csv.gz
├── locations/
│   └── locations.csv.gz
├── orders/
│   ├── orders.csv.gz
│   ├── orders_products.csv.gz
│   ├── orders_products_image.csv.gz
│   ├── orders_products_price.csv.gz
│   └── orders_status_history.csv.gz
├── products/
│   ├── products.csv.gz
│   ├── products_image.csv.gz
│   ├── products_price.csv.gz
│   └── products_vai_text_embedding.csv.gz
├── recommendations/
│   ├── recommendations.csv.gz
│   ├── recommendations_items.csv.gz
│   ├── recommendations_items_image.csv.gz
│   └── recommendations_items_price.csv.gz
└── users/
    ├── users.csv.gz
    ├── users_address.csv.gz
    └── users_lastrecommendations.csv.gz
```

### Loading into PostgreSQL

The CSV header row matches the SQL column names exactly, so you can load them
directly with `\COPY`:

```sql
\COPY products           FROM PROGRAM 'gunzip -c products.csv.gz'           CSV HEADER;
\COPY products_image     FROM PROGRAM 'gunzip -c products_image.csv.gz'     CSV HEADER;
\COPY products_price     FROM PROGRAM 'gunzip -c products_price.csv.gz'     CSV HEADER;

\COPY orders             FROM PROGRAM 'gunzip -c orders.csv.gz'             CSV HEADER;
\COPY orders_products    FROM PROGRAM 'gunzip -c orders_products.csv.gz'    CSV HEADER;
\COPY orders_status_history FROM PROGRAM 'gunzip -c orders_status_history.csv.gz' CSV HEADER;
```

Create tables

```bash
export PGURI="postgres://avnadmin:<redacted>@pg-testpmp-fras-d-pmp-todel.j.aivencloud.com:12833/defaultdb?sslmode=require"
find mongo2pg/mongodb-testpmp/schema/tables/sample_airbnb -maxdepth 1 -type f -name '*.sql' | while read -r f; do
  echo "executing psql -f $f"
  psql "$PGURI" -f $f
done
```

Load parent tables before child tables to satisfy foreign-key constraints.


```bash
{
  echo "BEGIN;"
  echo "SET CONSTRAINTS ALL DEFERRED;"
  find mongo2pg/mongodb-testpmp/data/sample_airbnb -mindepth 2 -maxdepth 2 -type f -name '*.csv.gz' | sort | while read -r f; do
    table=$(basename "$f" .csv.gz)
    printf "\\COPY %s FROM PROGRAM 'gunzip -c %s' CSV HEADER;\n" "$table" "$f"
  done
  echo "COMMIT;"
} | psql "$PGURI"
``` 

---

## Resulting project tree

After all five commands the project directory looks like this:

```
results/base/retail/
├── config/
│   └── retail.conf
├── data/
│   ├── carts/        ← carts.csv.gz, carts_products.csv.gz
│   ├── invoices/     ← invoices.csv.gz + 9 child table files
│   ├── locations/    ← locations.csv.gz
│   ├── orders/       ← orders.csv.gz + 4 child table files
│   ├── products/     ← products.csv.gz + 3 child table files
│   ├── recommendations/ ← recommendations.csv.gz + 3 child table files
│   └── users/        ← users.csv.gz + 2 child table files
├── reports/
│   ├── retail.html
│   └── retail.schema.html
├── schema/
│   └── tables/
│       ├── carts.sql
│       ├── invoices.sql
│       ├── locations.sql
│       ├── orders.sql
│       ├── products.sql
│       ├── recommendations.sql
│       └── users.sql
└── source/
    └── collections/
        ├── carts/
        │   ├── carts.json
        │   ├── carts.stats.txt
        │   └── carts.stats.yaml
        ├── invoices/  …
        ├── locations/ …
        ├── orders/    …
        ├── products/  …
        ├── recommendations/ …
        └── users/     …
```

you can see a diagram

![alt text](schema_diagram.png)

Example of report, with the corresponding pg tables created.

![alt text](retail.png)
