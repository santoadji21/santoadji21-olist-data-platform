"""Satu-satunya sumber kebenaran skema dataset Olist (T0).

Dipakai oleh: profiling (T0), generator kontrak ODCS (T0), silver typing (T2), dan DDL raw/dw (T2/T4).
Tipe target memakai nama tipe PostgreSQL agar bisa dipakai langsung sebagai DDL.
"""
from __future__ import annotations

from dataclasses import dataclass, field

RAW_DIR_DEFAULT = "data/raw"


@dataclass(frozen=True)
class Col:
    src: str                 # nama kolom di CSV sumber (apa adanya, termasuk typo)
    type: str                # tipe target (PostgreSQL)
    nullable: bool           # hasil profiling: ada null di sumber?
    desc: str
    note: str = ""           # catatan tambahan (aturan, rentang, dsb.)


@dataclass(frozen=True)
class Table:
    name: str                # nama tabel yang dipakai di raw/silver
    file: str                # nama file CSV
    rows: int                # jumlah baris diharapkan (parser CSV, bukan wc -l)
    key: tuple[str, ...]     # kunci alami; () = tidak ada kunci alami yang unik
    desc: str
    cols: tuple[Col, ...] = field(default_factory=tuple)
    encoding: str = "utf-8"

    @property
    def col_names(self) -> list[str]:
        return [c.src for c in self.cols]


TABLES: tuple[Table, ...] = (
    Table(
        "customers", "olist_customers_dataset.csv", 99_441, ("customer_id",),
        "Satu baris per ORDER (bukan per orang). customer_id bersifat per-order; "
        "customer_unique_id adalah identitas orang yang sebenarnya.",
        (
            Col("customer_id", "text", False, "ID pelanggan per order (join ke orders.customer_id)"),
            Col("customer_unique_id", "text", False, "ID orang sebenarnya; satu orang bisa punya banyak customer_id"),
            Col("customer_zip_code_prefix", "char(5)", False, "Prefix kode pos", "wajib teks 5 digit, lpad '0'"),
            Col("customer_city", "text", False, "Kota"),
            Col("customer_state", "char(2)", False, "Kode negara bagian (UF)"),
        ),
    ),
    Table(
        "geolocation", "olist_geolocation_dataset.csv", 1_000_163, (),
        "Koordinat per zip prefix. BANYAK baris per prefix (bukan kunci), banyak duplikat persis, "
        "dan ada titik di luar Brasil.",
        (
            Col("geolocation_zip_code_prefix", "char(5)", False, "Prefix kode pos", "wajib teks 5 digit, lpad '0'"),
            Col("geolocation_lat", "double precision", False, "Lintang", "Brasil ≈ −34 … +5"),
            Col("geolocation_lng", "double precision", False, "Bujur", "Brasil ≈ −74 … −35"),
            Col("geolocation_city", "text", False, "Kota (ejaan tidak seragam)"),
            Col("geolocation_state", "char(2)", False, "Kode negara bagian (UF)"),
        ),
    ),
    Table(
        "order_items", "olist_order_items_dataset.csv", 112_650, ("order_id", "order_item_id"),
        "Satu baris per unit barang dalam order.",
        (
            Col("order_id", "text", False, "FK → orders.order_id"),
            Col("order_item_id", "integer", False, "Nomor urut item dalam order (mulai 1)"),
            Col("product_id", "text", False, "FK → products.product_id"),
            Col("seller_id", "text", False, "FK → sellers.seller_id"),
            Col("shipping_limit_date", "timestamp", False, "Batas seller menyerahkan ke kurir (waktu lokal Brasil)"),
            Col("price", "numeric(10,2)", False, "Harga barang (BRL)", "≥ 0"),
            Col("freight_value", "numeric(10,2)", False, "Ongkir bagian item ini (BRL)", "≥ 0"),
        ),
    ),
    Table(
        "order_payments", "olist_order_payments_dataset.csv", 103_886, ("order_id", "payment_sequential"),
        "Pembayaran; satu order bisa dibayar dengan beberapa metode/cicilan.",
        (
            Col("order_id", "text", False, "FK → orders.order_id"),
            Col("payment_sequential", "integer", False, "Urutan pembayaran dalam order (mulai 1)"),
            Col("payment_type", "text", False, "credit_card | boleto | voucher | debit_card | not_defined"),
            Col("payment_installments", "integer", False, "Jumlah cicilan", "0 muncul di sumber"),
            Col("payment_value", "numeric(10,2)", False, "Nilai pembayaran (BRL)", "≥ 0; ada yang 0"),
        ),
    ),
    Table(
        "order_reviews", "olist_order_reviews_dataset.csv", 99_224, (),
        "Ulasan. review_id TIDAK unik, order_id TIDAK unik; komentar multi-baris (mengandung newline di dalam kutip).",
        (
            Col("review_id", "text", False, "ID ulasan (tidak unik)"),
            Col("order_id", "text", False, "FK → orders.order_id (satu order bisa punya > 1 ulasan)"),
            Col("review_score", "smallint", False, "Skor 1–5", "CHECK 1..5"),
            Col("review_comment_title", "text", True, "Judul komentar (kebanyakan kosong)"),
            Col("review_comment_message", "text", True, "Isi komentar (kebanyakan kosong)"),
            Col("review_creation_date", "timestamp", False, "Waktu survei dikirim"),
            Col("review_answer_timestamp", "timestamp", False, "Waktu survei dijawab"),
        ),
    ),
    Table(
        "orders", "olist_orders_dataset.csv", 99_441, ("order_id",),
        "Header order: status + stempel waktu. Tabel pusat; tiap order punya tepat satu baris.",
        (
            Col("order_id", "text", False, "Kunci alami"),
            Col("customer_id", "text", False, "FK → customers.customer_id (per order)"),
            Col("order_status", "text", False,
                "delivered | shipped | canceled | unavailable | invoiced | processing | created | approved"),
            Col("order_purchase_timestamp", "timestamp", False, "Waktu order dibuat"),
            Col("order_approved_at", "timestamp", True, "Waktu pembayaran disetujui"),
            Col("order_delivered_carrier_date", "timestamp", True, "Waktu diserahkan ke kurir"),
            Col("order_delivered_customer_date", "timestamp", True, "Waktu tiba di pelanggan"),
            Col("order_estimated_delivery_date", "timestamp", False,
                "Estimasi tiba (presisi tanggal; jam selalu 00:00:00)"),
        ),
    ),
    Table(
        "products", "olist_products_dataset.csv", 32_951, ("product_id",),
        "Katalog produk. Nama kolom sumber punya typo 'lenght'.",
        (
            Col("product_id", "text", False, "Kunci alami"),
            Col("product_category_name", "text", True, "Kategori (Portugis)"),
            Col("product_name_lenght", "integer", True, "Panjang nama", "typo sumber; dw: product_name_length"),
            Col("product_description_lenght", "integer", True, "Panjang deskripsi",
                "typo sumber; dw: product_description_length"),
            Col("product_photos_qty", "integer", True, "Jumlah foto"),
            Col("product_weight_g", "integer", True, "Berat (gram)"),
            Col("product_length_cm", "integer", True, "Panjang (cm)"),
            Col("product_height_cm", "integer", True, "Tinggi (cm)"),
            Col("product_width_cm", "integer", True, "Lebar (cm)"),
        ),
    ),
    Table(
        "sellers", "olist_sellers_dataset.csv", 3_095, ("seller_id",),
        "Seller + lokasi.",
        (
            Col("seller_id", "text", False, "Kunci alami"),
            Col("seller_zip_code_prefix", "char(5)", False, "Prefix kode pos", "wajib teks 5 digit, lpad '0'"),
            Col("seller_city", "text", False, "Kota"),
            Col("seller_state", "char(2)", False, "Kode negara bagian (UF)"),
        ),
    ),
    Table(
        "product_category_translation", "product_category_name_translation.csv", 71, ("product_category_name",),
        "Terjemahan kategori Portugis → Inggris. File diawali BOM UTF-8 (EF BB BF).",
        (
            Col("product_category_name", "text", False, "Kategori (Portugis)", "baca dengan utf-8-sig"),
            Col("product_category_name_english", "text", False, "Kategori (Inggris)"),
        ),
        encoding="utf-8-sig",
    ),
)

BY_NAME = {t.name: t for t in TABLES}

# Kolom waktu per tabel (untuk rentang tanggal di profil)
TIME_TYPES = {"timestamp", "date"}
NUMERIC_TYPES = ("integer", "smallint", "numeric", "double")

# Bounding box Brasil (dipakai profil & aturan kualitas; sama dengan petunjuk tugas)
BRAZIL_LAT = (-34.0, 5.0)
BRAZIL_LNG = (-74.0, -35.0)
