import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker for Russian names and cities
fake = Faker('ru_RU')

# --- CONFIGURATION ---
NUM_CLIENTS = 60
START_DATE = datetime(2024, 6, 1)
END_DATE = datetime(2024, 8, 31)

# --- DATA DEFINITIONS ---
CLIENT_STATUSES = ['Студент', 'Зарплатный клиент', 'Премиальный клиент', 'Стандартный клиент']
CITIES = ['Алматы', 'Астана', 'Шымкент', 'Караганда', 'Актобе']

TRANSACTION_CATEGORIES = [
    'Одежда и обувь', 'Продукты питания', 'Кафе и рестораны', 'Медицина', 'Авто', 'Спорт',
    'Развлечения', 'АЗС', 'Кино', 'Питомцы', 'Книги', 'Цветы', 'Едим дома', 'Смотрим дома',
    'Играем дома', 'Косметика и Парфюмерия', 'Подарки', 'Ремонт дома', 'Мебель',
    'Спа и массаж', 'Ювелирные украшения', 'Такси', 'Отели', 'Путешествия'
]

TRANSFER_TYPES = [
    'salary_in', 'stipend_in', 'family_in', 'cashback_in', 'refund_in', 'card_in',
    'p2p_out', 'card_out', 'atm_withdrawal', 'utilities_out', 'loan_payment_out',
    'cc_repayment_out', 'installment_payment_out', 'fx_buy', 'fx_sell', 'invest_out',
    'invest_in', 'deposit_topup_out', 'deposit_fx_topup_out', 'deposit_fx_withdraw_in',
    'gold_buy_out', 'gold_sell_in'
]

# --- HELPER FUNCTIONS to create biased data based on client profile ---

def get_balance_for_status(status):
    if status == 'Студент':
        return random.randint(10000, 150000)
    if status == 'Стандартный клиент':
        return random.randint(50000, 500000)
    if status == 'Зарплатный клиент':
        return random.randint(150000, 1000000)
    if status == 'Премиальный клиент':
        return random.randint(1000000, 10000000)
    return 0

def get_age_for_status(status):
    if status == 'Студент':
        return random.randint(18, 22)
    if status == 'Стандартный клиент':
        return random.randint(23, 60)
    if status == 'Зарплатный клиент':
        return random.randint(25, 55)
    if status == 'Премиальный клиент':
        return random.randint(35, 65)
    return 30

def generate_transactions_for_client(client_code, client_status, num_transactions=50):
    transactions = []
    days_in_period = (END_DATE - START_DATE).days

    # Define spending profiles
    spending_profile = {
        'Студент': {'Продукты питания': 0.3, 'Кафе и рестораны': 0.2, 'Развлечения': 0.15, 'Такси': 0.1, 'Смотрим дома': 0.1, 'Одежда и обувь': 0.05, 'Книги': 0.05, 'Другое': 0.05},
        'Зарплатный клиент': {'Продукты питания': 0.4, 'АЗС': 0.15, 'Медицина': 0.1, 'Одежда и обувь': 0.1, 'Ремонт дома': 0.1, 'Кафе и рестораны': 0.05, 'Другое': 0.1},
        'Премиальный клиент': {'Кафе и рестораны': 0.2, 'Путешествия': 0.2, 'Отели': 0.1, 'Ювелирные украшения': 0.1, 'Косметика и Парфюмерия': 0.1, 'Такси': 0.1, 'Авто': 0.1, 'Другое': 0.1},
        'Стандартный клиент': {'Продукты питания': 0.3, 'Одежда и обувь': 0.15, 'Кафе и рестораны': 0.1, 'Медицина': 0.1, 'Развлечения': 0.1, 'Такси': 0.1, 'Другое': 0.15}
    }

    # Pick a profile, default to standard
    profile_weights = spending_profile.get(client_status, spending_profile['Стандартный клиент'])
    categories = list(profile_weights.keys())
    weights = list(profile_weights.values())

    for _ in range(num_transactions):
        # Choose category based on profile
        if 'Другое' in categories:
            # handle 'Другое' case
            other_cats = [c for c in TRANSACTION_CATEGORIES if c not in categories]

            # The population should not include 'Другое' itself
            population_cats = categories[:-1] + other_cats

            # The weights for 'Другое' should be distributed among the other_cats
            weight_for_other = weights[-1]
            if not other_cats: # Avoid division by zero if all categories are in profile
                chosen_cat = random.choices(categories[:-1], weights=weights[:-1])[0]
            else:
                weights_dist = weights[:-1] + [weight_for_other / len(other_cats)] * len(other_cats)
                chosen_cat = random.choices(population_cats, weights=weights_dist)[0]
        else:
            chosen_cat = random.choices(categories, weights=weights)[0]

        # Generate amount
        amount = 0
        if chosen_cat in ['Путешествия', 'Отели', 'Мебель', 'Ювелирные украшения']:
            amount = random.randint(50000, 500000)
        elif chosen_cat in ['Продукты питания', 'Такси', 'Кино']:
             amount = random.randint(1000, 15000)
        else:
            amount = random.randint(5000, 50000)

        # Generate date
        random_day = random.randint(0, days_in_period)
        date = START_DATE + timedelta(days=random_day)

        # Currency
        currency = 'KZT'
        if chosen_cat in ['Путешествия', 'Отели'] and random.random() < 0.3: # 30% chance of foreign transaction
            currency = random.choice(['USD', 'EUR'])
            amount = round(amount / 450, 2) if currency == 'USD' else round(amount/500, 2)

        transactions.append({
            'date': date.strftime('%Y-%m-%d'),
            'category': chosen_cat,
            'amount': amount,
            'currency': currency,
            'client_code': client_code
        })

    return transactions

def generate_transfers_for_client(client_code, client_status, num_transfers=20):
    transfers = []
    days_in_period = (END_DATE - START_DATE).days

    # Define transfer profiles
    transfer_profile = {
        'Студент': {'stipend_in': 0.3, 'family_in': 0.3, 'p2p_out': 0.2, 'card_out': 0.2},
        'Зарплатный клиент': {'salary_in': 0.4, 'p2p_out': 0.2, 'atm_withdrawal': 0.1, 'utilities_out': 0.1, 'loan_payment_out': 0.1, 'deposit_topup_out': 0.1},
        'Премиальный клиент': {'salary_in': 0.2, 'invest_in': 0.2, 'fx_buy': 0.1, 'fx_sell': 0.1, 'p2p_out': 0.2, 'deposit_fx_topup_out': 0.1, 'gold_buy_out': 0.1},
        'Стандартный клиент': {'salary_in': 0.3, 'p2p_out': 0.3, 'atm_withdrawal': 0.1, 'utilities_out': 0.1, 'cc_repayment_out': 0.1, 'card_in': 0.1}
    }

    profile_weights = transfer_profile.get(client_status, transfer_profile['Стандартный клиент'])
    types = list(profile_weights.keys())
    weights = list(profile_weights.values())

    for _ in range(num_transfers):
        transfer_type = random.choices(types, weights=weights)[0]

        direction = 'in' if '_in' in transfer_type else 'out'

        # Generate amount
        amount = 0
        if 'salary' in transfer_type or 'invest' in transfer_type or 'gold' in transfer_type:
            amount = random.randint(150000, 1000000)
        elif 'stipend' in transfer_type:
            amount = random.randint(20000, 50000)
        else:
            amount = random.randint(10000, 100000)

        # Generate date
        random_day = random.randint(0, days_in_period)
        date = START_DATE + timedelta(days=random_day)

        transfers.append({
            'date': date.strftime('%Y-%m-%d'),
            'type': transfer_type,
            'direction': direction,
            'amount': amount,
            'currency': 'KZT',
            'client_code': client_code
        })

    return transfers


# --- MAIN SCRIPT ---
def generate_data():
    print("Generating client profiles...")
    clients = []
    for i in range(1, NUM_CLIENTS + 1):
        status = random.choices(CLIENT_STATUSES, weights=[0.15, 0.4, 0.1, 0.35], k=1)[0]
        clients.append({
            'client_code': i,
            'name': fake.first_name(),
            'status': status,
            'age': get_age_for_status(status),
            'city': random.choice(CITIES),
            'avg_monthly_balance_KZT': get_balance_for_status(status)
        })
    clients_df = pd.DataFrame(clients)

    print("Generating transactions and transfers...")
    all_transactions = []
    all_transfers = []
    for _, client in clients_df.iterrows():
        # Vary number of transactions/transfers based on status
        num_trans = random.randint(20, 80) if client['status'] in ['Премиальный клиент', 'Зарплатный клиент'] else random.randint(10, 40)
        num_transf = random.randint(10, 40) if client['status'] in ['Премиальный клиент', 'Зарплатный клиент'] else random.randint(5, 20)

        all_transactions.extend(generate_transactions_for_client(client['client_code'], client['status'], num_trans))
        all_transfers.extend(generate_transfers_for_client(client['client_code'], client['status'], num_transf))

    transactions_df = pd.DataFrame(all_transactions)
    transfers_df = pd.DataFrame(all_transfers)

    print(f"Generated {len(clients_df)} clients.")
    print(f"Generated {len(transactions_df)} transactions.")
    print(f"Generated {len(transfers_df)} transfers.")

    # Save to CSV
    clients_df.to_csv('clients.csv', index=False)
    transactions_df.to_csv('transactions.csv', index=False)
    transfers_df.to_csv('transfers.csv', index=False)

    print("Data generation complete. Files saved to clients.csv, transactions.csv, transfers.csv")

if __name__ == '__main__':
    generate_data()
