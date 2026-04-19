import csv
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

random.seed(42)


NUM_CUSTOMERS = 2000
NUM_PRODUCTS = 150
NUM_ORDERS = 5000
MAX_ITEMS_PER_ORDER = 5

# Date ranges
SIGNUP_START = datetime(2020, 1, 1)
SIGNUP_END = datetime(2024, 12, 31)
ORDER_START = datetime(2022, 1, 1)
ORDER_END = datetime(2025, 3, 31)


VALID_STATUSES = ["CREATED", "SHIPPED", "DELIVERED", "CANCELLED"]

INVALID_STATUSES = ["PENDING", "RETURNED", "UNKNOWN", "pending", "delivered"]


OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


FIRST_NAMES = [
    "Aarav", "Aditi", "Amit", "Ananya", "Arjun", "Deepa", "Divya", "Gaurav",
    "Isha", "Karan", "Kavya", "Lakshmi", "Manish", "Neha", "Nikhil", "Pooja",
    "Priya", "Rahul", "Riya", "Rohit", "Sakshi", "Sandeep", "Shreya", "Siddharth",
    "Sneha", "Sunita", "Suresh", "Tanvi", "Varun", "Vikram", "James", "Mary",
    "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "David", "Sarah",
    "William", "Karen", "Richard", "Lisa", "Joseph", "Nancy", "Thomas", "Betty",
    "Emma", "Oliver", "Charlotte", "Liam", "Sophia", "Noah", "Isabella", "Lucas",
    "Mia", "Ethan", "Amelia", "Alexander", "Harper", "Daniel", "Evelyn", "Matthew",
    "Abigail", "Sebastian", "Ella", "Benjamin", "Scarlett", "Henry", "Grace"
]

LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Kumar", "Verma", "Gupta", "Reddy", "Nair",
    "Mehta", "Joshi", "Shah", "Rao", "Iyer", "Desai", "Bhat", "Pillai",
    "Chopra", "Malhotra", "Kapoor", "Agarwal", "Smith", "Johnson", "Williams",
    "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "White",
    "Harris", "Thompson", "Clark", "Lewis", "Walker", "Hall", "Allen", "Young"
]

COUNTRIES = [
    "India", "United States", "United Kingdom", "Canada", "Australia",
    "Germany", "France", "Japan", "Singapore", "UAE",
    "Netherlands", "Sweden", "Brazil", "South Korea", "Ireland"
]

# Weighted distribution — most customers from top 3 countries
COUNTRY_WEIGHTS = [35, 25, 10, 5, 5, 4, 3, 3, 2, 2, 1, 1, 1, 1, 2]

CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books", "Sports & Outdoors",
    "Beauty & Personal Care", "Toys & Games", "Grocery", "Office Supplies",
    "Automotive", "Health & Wellness", "Pet Supplies"
]

PRODUCT_PREFIXES = {
    "Electronics": ["Wireless", "Smart", "Bluetooth", "HD", "Portable", "Digital", "Pro"],
    "Clothing": ["Cotton", "Slim Fit", "Classic", "Premium", "Casual", "Formal", "Vintage"],
    "Home & Kitchen": ["Stainless Steel", "Non-Stick", "Bamboo", "Ceramic", "Glass", "Wooden", "Silicone"],
    "Books": ["The Art of", "Introduction to", "Advanced", "Complete Guide to", "Mastering", "Essential"],
    "Sports & Outdoors": ["Ultra Light", "Pro Series", "All-Weather", "Heavy Duty", "Compact", "Foldable"],
    "Beauty & Personal Care": ["Organic", "Natural", "Daily", "Hydrating", "Anti-Aging", "Gentle"],
    "Toys & Games": ["Classic", "Educational", "Interactive", "Creative", "Strategy", "Adventure"],
    "Grocery": ["Organic", "Whole Grain", "Sugar-Free", "Gluten-Free", "Premium", "Farm Fresh"],
    "Office Supplies": ["Ergonomic", "Recycled", "Premium", "Professional", "Heavy Duty", "Compact"],
    "Automotive": ["Universal", "All-Season", "Heavy Duty", "Premium", "Quick-Release", "Anti-Slip"],
    "Health & Wellness": ["Daily", "Natural", "Advanced", "Essential", "Clinical", "Pure"],
    "Pet Supplies": ["Organic", "Natural", "Durable", "Interactive", "Comfortable", "Premium"]
}

PRODUCT_ITEMS = {
    "Electronics": ["Headphones", "Mouse", "Keyboard", "Speaker", "Charger", "Camera", "Tablet Stand", "USB Hub", "Monitor Light", "Webcam"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Hoodie", "Shorts", "Polo Shirt", "Sweater", "Trousers", "Shirt", "Vest"],
    "Home & Kitchen": ["Pan", "Knife Set", "Cutting Board", "Blender", "Toaster", "Kettle", "Storage Container", "Spatula Set", "Mixing Bowl", "Timer"],
    "Books": ["Python Programming", "Data Science", "Machine Learning", "Cloud Computing", "System Design", "Algorithms"],
    "Sports & Outdoors": ["Water Bottle", "Yoga Mat", "Resistance Band", "Jump Rope", "Backpack", "Tent", "Flashlight", "Compass"],
    "Beauty & Personal Care": ["Face Wash", "Moisturizer", "Sunscreen", "Shampoo", "Conditioner", "Body Lotion", "Lip Balm", "Face Mask"],
    "Toys & Games": ["Board Game", "Puzzle Set", "Building Blocks", "Card Game", "Action Figure", "Dollhouse", "RC Car", "Chess Set"],
    "Grocery": ["Granola", "Trail Mix", "Honey", "Olive Oil", "Coffee Beans", "Tea Collection", "Pasta", "Rice"],
    "Office Supplies": ["Notebook", "Pen Set", "Desk Organizer", "Paper Clips", "Stapler", "Highlighters", "Sticky Notes", "File Folders"],
    "Automotive": ["Phone Mount", "Floor Mats", "Air Freshener", "Seat Cover", "Dash Cam", "Tire Gauge", "Jump Starter", "Wiper Blades"],
    "Health & Wellness": ["Multivitamin", "Protein Powder", "Fish Oil", "Probiotic", "Vitamin D", "Zinc Tablets", "First Aid Kit", "Thermometer"],
    "Pet Supplies": ["Dog Food", "Cat Toy", "Pet Bed", "Leash", "Food Bowl", "Grooming Kit", "Collar", "Treat Jar"]
}


PRICE_RANGES = {
    "Electronics": (9.99, 299.99),
    "Clothing": (12.99, 149.99),
    "Home & Kitchen": (7.99, 199.99),
    "Books": (4.99, 49.99),
    "Sports & Outdoors": (8.99, 179.99),
    "Beauty & Personal Care": (3.99, 79.99),
    "Toys & Games": (6.99, 89.99),
    "Grocery": (2.99, 39.99),
    "Office Supplies": (1.99, 59.99),
    "Automotive": (5.99, 149.99),
    "Health & Wellness": (7.99, 59.99),
    "Pet Supplies": (4.99, 89.99)
}

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "protonmail.com", "mail.com", "zoho.com", "aol.com", "yandex.com"
]



def random_date(start: datetime, end: datetime) -> str:
    """Generate a random date between start and end as YYYY-MM-DD string."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    dt = start + timedelta(days=random_days)
    return dt.strftime("%Y-%m-%d")


def generate_email(first_name: str, last_name: str, customer_id: int) -> str:
    """Generate a realistic email address."""
    domain = random.choice(EMAIL_DOMAINS)
    patterns = [
        f"{first_name.lower()}.{last_name.lower()}@{domain}",
        f"{first_name.lower()}{last_name.lower()[:3]}@{domain}",
        f"{first_name.lower()[0]}{last_name.lower()}@{domain}",
        f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{domain}",
        f"{first_name.lower()}_{last_name.lower()}@{domain}",
    ]
    return random.choice(patterns)


def inject_dirty_email(email: str) -> str:
    """Corrupt an email to test validation logic."""
    corruptions = [
        email.replace("@", ""),          
        email.replace("@", "@@"),        
        email.replace(".", ""),          
        f" {email}",                      
        "not_an_email",                  
        "",                                
        email.replace("@", " at "),      
    ]
    return random.choice(corruptions)


def add_whitespace_noise(text: str) -> str:
    """Add leading/trailing whitespace to ~5% of text values."""
    if random.random() < 0.05:
        noise = random.choice([
            f"  {text}",
            f"{text}  ",
            f"  {text}  ",
            f" {text}",
        ])
        return noise
    return text


def randomize_case(text: str) -> str:
    """Randomize casing for ~3% of text values to test standardization."""
    if random.random() < 0.03:
        return random.choice([text.upper(), text.lower(), text.title(), text.swapcase()])
    return text



def generate_customers() -> List[Dict[str, Any]]:
    """
    Generate customer records with deliberate quality issues.
   
    Issues injected:
    - ~2% duplicate customer_ids (same ID, slightly different data)
    - ~3% invalid email formats
    - ~5% whitespace noise in names
    - ~3% case inconsistencies in names
    - ~1% future signup dates
    - ~1% NULL/empty country values
    """
    print(f"Generating {NUM_CUSTOMERS} customers...")
    customers = []
   
    for i in range(1, NUM_CUSTOMERS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = generate_email(first_name, last_name, i)
       
       
        if random.random() < 0.03:
            email = inject_dirty_email(email)
       
       
        first_name = add_whitespace_noise(first_name)
        last_name = add_whitespace_noise(last_name)
       
       
        first_name = randomize_case(first_name)
        last_name = randomize_case(last_name)
       
        signup_date = random_date(SIGNUP_START, SIGNUP_END)
       
       
        if random.random() < 0.01:
            future_start = datetime(2026, 1, 1)
            future_end = datetime(2027, 12, 31)
            signup_date = random_date(future_start, future_end)
       
       
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        if random.random() < 0.01:
            country = ""
       
        customers.append({
            "customer_id": i,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "signup_date": signup_date,
            "country": country
        })
   
   
    num_duplicates = int(NUM_CUSTOMERS * 0.02)
    for _ in range(num_duplicates):
        original = random.choice(customers[:NUM_CUSTOMERS])
        duplicate = original.copy()
       
        duplicate["email"] = generate_email(
            duplicate["first_name"].strip(),
            duplicate["last_name"].strip(),
            duplicate["customer_id"]
        )
        customers.append(duplicate)
   
   
    random.shuffle(customers)
   
    print(f"  → {len(customers)} rows (including {num_duplicates} duplicates)")
    return customers


def generate_products() -> List[Dict[str, Any]]:
    """
    Generate product records with deliberate quality issues.
   
    Issues injected:
    - ~2% negative or zero prices
    - ~3% case inconsistencies in category names
    - ~2% whitespace noise in product names
    - Realistic price distribution per category
    """
    print(f"Generating {NUM_PRODUCTS} products...")
    products = []
   
    product_id = 1
   
    products_per_category = NUM_PRODUCTS // len(CATEGORIES)
    remainder = NUM_PRODUCTS % len(CATEGORIES)
   
    for idx, category in enumerate(CATEGORIES):
        count = products_per_category + (1 if idx < remainder else 0)
        prefixes = PRODUCT_PREFIXES[category]
        items = PRODUCT_ITEMS[category]
        price_min, price_max = PRICE_RANGES[category]
       
        for _ in range(count):
            prefix = random.choice(prefixes)
            item = random.choice(items)
            product_name = f"{prefix} {item}"
           
           
            product_name = add_whitespace_noise(product_name)
           
           
            price = round(random.uniform(price_min, price_max), 2)
           
           
            if random.random() < 0.02:
                price = random.choice([0, -1.00, -price, -0.01, 0.00])
           
           
            cat_name = randomize_case(category)
           
            products.append({
                "product_id": product_id,
                "product_name": product_name,
                "category": cat_name,
                "price": price
            })
            product_id += 1
   
    print(f"  → {len(products)} rows")
    return products


def generate_orders(customer_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Generate order records with deliberate quality issues.
   
    Issues injected:
    - ~1.5% NULL order dates
    - ~1% invalid statuses
    - ~2% future order dates
    - Realistic status distribution (most orders are DELIVERED)
    """
    print(f"Generating {NUM_ORDERS} orders...")
    orders = []
   
   
    status_weights = [15, 20, 50, 15]  
   
    for i in range(1, NUM_ORDERS + 1):
        customer_id = random.choice(customer_ids)
        order_date = random_date(ORDER_START, ORDER_END)
        status = random.choices(VALID_STATUSES, weights=status_weights, k=1)[0]
       
       
        if random.random() < 0.015:
            order_date = ""
       
       
        if random.random() < 0.02 and order_date != "":
            future_start = datetime(2026, 6, 1)
            future_end = datetime(2027, 12, 31)
            order_date = random_date(future_start, future_end)
       
       
        if random.random() < 0.01:
            status = random.choice(INVALID_STATUSES)
       
        orders.append({
            "order_id": i,
            "customer_id": customer_id,
            "order_date": order_date,
            "status": status
        })
   
    print(f"  → {len(orders)} rows")
    return orders


def generate_order_items(order_ids: List[int], product_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Generate order item records with deliberate quality issues.
   
    Issues injected:
    - ~1% orphan records (referencing non-existent order_ids)
    - ~0.5% zero or negative quantities
    - Realistic quantity distribution (most orders have 1-3 items)
    """
    print("Generating order items...")
    order_items = []
    item_id = 1
   
    max_order_id = max(order_ids)
   
    for order_id in order_ids:
       
        num_items = random.choices(
            [1, 2, 3, 4, 5],
            weights=[35, 30, 20, 10, 5],
            k=1
        )[0]
       
       
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
       
        for product_id in selected_products:
            quantity = random.choices(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                weights=[30, 25, 15, 10, 8, 5, 3, 2, 1, 1],
                k=1
            )[0]
           
           
            if random.random() < 0.005:
                quantity = random.choice([0, -1, -2])
           
            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity
            })
            item_id += 1
   
   
    num_orphans = int(len(order_items) * 0.01)
    for _ in range(num_orphans):
        orphan_order_id = max_order_id + random.randint(1, 1000)
        order_items.append({
            "order_item_id": item_id,
            "order_id": orphan_order_id,
            "product_id": random.choice(product_ids),
            "quantity": random.randint(1, 5)
        })
        item_id += 1
   
    random.shuffle(order_items)
   
    print(f"  → {len(order_items)} rows (including {num_orphans} orphans)")
    return order_items


def write_csv(filepath: str, data: List[Dict[str, Any]], fieldnames: List[str]):
    """Write data to CSV with proper quoting."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(data)
    file_size = os.path.getsize(filepath)
    print(f"  ✓ Written: {filepath} ({file_size:,} bytes)")




def main():
    """Generate all datasets and write to CSV files."""
    print("=" * 60)
    print("RETAIL DATASET GENERATOR")
    print("=" * 60)
    print()
   
   
    customers = generate_customers()
    customer_ids = list(set(c["customer_id"] for c in customers))
   
   
    products = generate_products()
    product_ids = [p["product_id"] for p in products]
   
   
    orders = generate_orders(customer_ids)
    order_ids = [o["order_id"] for o in orders]
   
   
    order_items = generate_order_items(order_ids, product_ids)
   
    print()
    print("Writing CSV files...")
   
    write_csv(
        os.path.join(OUTPUT_DIR, "customers.csv"),
        customers,
        ["customer_id", "first_name", "last_name", "email", "signup_date", "country"]
    )
   
    write_csv(
        os.path.join(OUTPUT_DIR, "products.csv"),
        products,
        ["product_id", "product_name", "category", "price"]
    )
   
    write_csv(
        os.path.join(OUTPUT_DIR, "orders.csv"),
        orders,
        ["order_id", "customer_id", "order_date", "status"]
    )
   
    write_csv(
        os.path.join(OUTPUT_DIR, "order_items.csv"),
        order_items,
        ["order_item_id", "order_id", "product_id", "quantity"]
    )
   
    print()
    print("=" * 60)
    print("DATA GENERATION COMPLETE")
    print("=" * 60)
    print()
    print("Summary of injected data quality issues:")
    print("  • Customers: ~2% duplicate IDs, ~3% invalid emails, whitespace & case noise")
    print("  • Products: ~2% invalid prices (zero/negative), category case noise")
    print("  • Orders: ~1.5% NULL dates, ~1% invalid statuses, ~2% future dates")
    print("  • Order Items: ~1% orphan records, ~0.5% invalid quantities")
    print()
    print("These issues will be caught and cleaned in the Silver layer.")


if __name__ == "__main__":
    main()
