import pandas as pd

def analyze_clients():
    """
    Main function to analyze client data and generate push notification recommendations.
    """
    # Step 3: Implement Data Loading and Processing
    try:
        clients_df = pd.read_csv('clients.csv')
        transactions_df = pd.read_csv('transactions.csv')
        transfers_df = pd.read_csv('transfers.csv')
    except FileNotFoundError as e:
        print(f"Error: {e}. Please run data_generator.py first to generate the data.")
        return

    # --- Data Preprocessing ---
    # Convert date columns to datetime objects for proper analysis
    transactions_df['date'] = pd.to_datetime(transactions_df['date'])
    transfers_df['date'] = pd.to_datetime(transfers_df['date'])

    print("Data loaded and preprocessed successfully.")

    recommendations = []

    for _, client_profile in clients_df.iterrows():
        client_code = client_profile['client_code']
        client_transactions = transactions_df[transactions_df['client_code'] == client_code]
        client_transfers = transfers_df[transfers_df['client_code'] == client_code]

        # Step 4: Implement Benefit Calculation
        all_benefits = {}
        all_benefits['Карта для путешествий'] = calculate_travel_card_benefit(client_transactions)
        all_benefits['Премиальная карта'] = calculate_premium_card_benefit(client_profile, client_transactions, client_transfers)
        all_benefits['Кредитная карта'] = calculate_credit_card_benefit(client_transactions.copy()) # Pass copy to avoid SettingWithCopyWarning
        all_benefits['Обмен валют'] = calculate_fx_benefit(client_transfers)
        all_benefits['Депозит сберегательный'] = calculate_savings_deposit_benefit(client_profile)
        all_benefits['Депозит накопительный'] = calculate_accumulation_deposit_benefit(client_profile, client_transfers)

        # Scoring-based products are converted to a monetary equivalent for comparison
        score_to_money_factor = 2500
        all_benefits['Кредит наличными'] = score_cash_loan_need(client_profile, client_transfers) * score_to_money_factor
        all_benefits['Депозит мультивалютный'] = score_multicurrency_deposit_need(client_profile, client_transactions, client_transfers) * score_to_money_factor
        all_benefits['Инвестиции'] = score_investment_need(client_profile) * score_to_money_factor
        all_benefits['Золотые слитки'] = score_gold_need(client_profile) * score_to_money_factor

        # Step 5: Implement Product Selection
        # Filter out products with zero or negative benefit before selecting the best one
        positive_benefits = {k: v for k, v in all_benefits.items() if v > 0}

        if not positive_benefits:
            # No suitable product found for this client
            best_product = "Нет подходящего продукта"
            benefit_value = 0
        else:
            best_product = max(positive_benefits, key=positive_benefits.get)
            benefit_value = positive_benefits[best_product]

        # Step 6: Implement Push Notification Generation
        push_notification = generate_push_notification(client_profile, best_product, benefit_value, client_transactions.copy(), client_transfers.copy())

        # Only add recommendation if a product was found
        if best_product != "Нет подходящего продукта":
            recommendations.append({
                'client_code': client_code,
                'product': best_product,
                'push_notification': push_notification
            })

    # Step 7: Generate Final CSV
    if recommendations:
        recommendations_df = pd.DataFrame(recommendations)
        recommendations_df.to_csv('recommendations.csv', index=False)
        print(f"Recommendations generated for {len(recommendations_df)} clients and saved to recommendations.csv")
    else:
        print("No recommendations were generated.")

# --- Constants for Benefit Calculation ---
FX_RATES = {'USD': 450, 'EUR': 500, 'KZT': 1}
TRAVEL_CARD_CASHBACK_RATE = 0.07
PREMIUM_CARD_JEWELRY_CASHBACK_RATE = 0.04
CREDIT_CARD_CASHBACK_RATE = 0.03
FX_SAVING_RATE = 0.005 # Assumed saving on spread
SAVINGS_DEPOSIT_RATE = 0.14 / 4 # Quarterly rate for 3 months of data
ACCUMULATION_DEPOSIT_RATE = 0.12 / 4 # Quarterly rate
PREMIUM_TIER_BALANCE_THRESHOLD = 1000000
PREMIUM_TIER_CASHBACK_RATE = 0.02
BASE_TIER_CASHBACK_RATE = 0.01
ATM_FEE_SAVING = 500 # Assumed saving per withdrawal
P2P_FEE_SAVING = 150 # Assumed saving per p2p transfer

# --- Benefit Calculation Functions ---

def get_spend_for_categories(transactions, categories):
    """Calculates total spend in specified categories, converting currencies."""
    if transactions.empty:
        return 0

    # Create a temporary column for amount in KZT
    transactions['amount_kzt'] = transactions.apply(lambda row: row['amount'] * FX_RATES.get(row['currency'], 1), axis=1)

    spend = transactions[transactions['category'].isin(categories)]['amount_kzt'].sum()
    return spend

def calculate_travel_card_benefit(transactions):
    """Benefit = 4% * spend(Путешествия + Такси + Отели)"""
    travel_categories = ['Путешествия', 'Такси', 'Отели']
    spend = get_spend_for_categories(transactions.copy(), travel_categories)
    return spend * TRAVEL_CARD_CASHBACK_RATE

def calculate_premium_card_benefit(profile, transactions, transfers):
    """Benefit = tier_cashback * base_spend + 4% * spend(jewelry+cosmetics+restaurants) + saved_fees(ATM, transfers)"""
    balance = profile['avg_monthly_balance_KZT']
    tier_rate = PREMIUM_TIER_CASHBACK_RATE if balance >= PREMIUM_TIER_BALANCE_THRESHOLD else BASE_TIER_CASHBACK_RATE

    premium_categories = ['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны']
    all_categories = transactions['category'].unique()
    non_premium_categories = [c for c in all_categories if c not in premium_categories]

    non_premium_spend = get_spend_for_categories(transactions.copy(), non_premium_categories)
    tier_cashback = non_premium_spend * tier_rate

    premium_spend = get_spend_for_categories(transactions.copy(), premium_categories)
    premium_cashback = premium_spend * PREMIUM_CARD_JEWELRY_CASHBACK_RATE

    atm_withdrawals = len(transfers[transfers['type'] == 'atm_withdrawal'])
    p2p_transfers = len(transfers[transfers['type'] == 'p2p_out'])
    saved_fees = (atm_withdrawals * ATM_FEE_SAVING) + (p2p_transfers * P2P_FEE_SAVING)

    return tier_cashback + premium_cashback + saved_fees

def calculate_credit_card_benefit(transactions):
    """Benefit = 10% in 3 «любимых» категориях + 10% на онлайн-услуги"""
    if transactions.empty:
        return 0

    transactions['amount_kzt'] = transactions.apply(lambda row: row['amount'] * FX_RATES.get(row['currency'], 1), axis=1)

    top_3_cats = transactions.groupby('category')['amount_kzt'].sum().nlargest(3).index.tolist()
    top_3_spend = transactions[transactions['category'].isin(top_3_cats)]['amount_kzt'].sum()

    online_categories = ['Едим дома', 'Смотрим дома', 'Играем дома']
    online_spend = transactions[transactions['category'].isin(online_categories)]['amount_kzt'].sum()

    return (top_3_spend + online_spend) * CREDIT_CARD_CASHBACK_RATE

def calculate_fx_benefit(transfers):
    """Benefit = 0.5% on fx_buy/fx_sell volumes"""
    fx_volume = transfers[transfers['type'].isin(['fx_buy', 'fx_sell'])]['amount'].sum()
    return fx_volume * FX_SAVING_RATE

def score_cash_loan_need(profile, transfers):
    """Scores need based on low balance, high outflows vs inflows, and existing loans."""
    score = 0
    if profile['avg_monthly_balance_KZT'] < 50000:
        score += 1

    inflows = transfers[transfers['direction'] == 'in']['amount'].sum()
    outflows = transfers[transfers['direction'] == 'out']['amount'].sum()
    if outflows > inflows * 1.5 and inflows > 0: # Avoid division by zero
        score += 2

    if 'loan_payment_out' in transfers['type'].unique():
        score += 1

    return score

def score_multicurrency_deposit_need(profile, transactions, transfers):
    """Scores need based on free balance, FX activity, and foreign spend."""
    score = 0
    if profile['avg_monthly_balance_KZT'] > 300000:
        score += 1
    if not transfers[transfers['type'].isin(['fx_buy', 'fx_sell'])].empty:
        score += 2
    if not transactions[transactions['currency'] != 'KZT'].empty:
        score += 2
    return score

def calculate_savings_deposit_benefit(profile):
    """Benefit = 14% annual on avg balance, for stable clients"""
    if profile['status'] != 'Студент' and profile['avg_monthly_balance_KZT'] > 750000:
        return profile['avg_monthly_balance_KZT'] * SAVINGS_DEPOSIT_RATE
    return 0

def calculate_accumulation_deposit_benefit(profile, transfers):
    """Benefit = 12% annual on avg balance, for clients with regular top-ups."""
    has_topups = 'deposit_topup_out' in transfers['type'].unique()
    if has_topups and profile['avg_monthly_balance_KZT'] > 50000:
         return profile['avg_monthly_balance_KZT'] * ACCUMULATION_DEPOSIT_RATE
    return 0

def score_investment_need(profile):
    """Scores need based on free cash."""
    balance = profile['avg_monthly_balance_KZT']
    if balance > 500000: return 3
    if balance > 200000: return 2
    if balance > 100000: return 1
    return 0

def score_gold_need(profile):
    """Scores need based on high liquidity."""
    if profile['avg_monthly_balance_KZT'] > 2000000: return 2
    if profile['status'] == 'Премиальный клиент': return 1
    return 0

# --- Push Notification Generation ---

def format_kzt(amount):
    """Formats a number as KZT currency, e.g., 2 490 ₸."""
    # Round to nearest integer and then format
    return f"{int(round(amount, 0)):,}".replace(",", " ") + " ₸"

def generate_push_notification(client, product, benefit, transactions, transfers):
    """Generates a personalized push notification based on the recommended product."""
    name = client['name']

    # --- Template for Карта для путешествий ---
    if product == 'Карта для путешествий':
        # Find the month with the most travel spend
        if not transactions.empty and 'amount_kzt' in transactions.columns:
            transactions['month'] = transactions['date'].dt.month
            travel_cats = ['Путешествия', 'Такси', 'Отели']
            spend_by_month = transactions[transactions['category'].isin(travel_cats)].groupby('month')['amount_kzt'].sum()
            if not spend_by_month.empty:
                top_month_num = spend_by_month.idxmax()
                months = {6: "июне", 7: "июле", 8: "августе"}
                top_month_name = months.get(top_month_num, "последние 3 месяца")
                return f"{name}, в {top_month_name} вы много путешествовали. С тревел-картой вернули бы кешбэком ≈{format_kzt(benefit)}. Оформить карту."
        return f"{name}, часто ездите на такси и путешествуете? С нашей тревел-картой могли бы получать до {int(TRAVEL_CARD_CASHBACK_RATE*100)}% кешбэка. Оформить карту."

    # --- Template for Премиальная карта ---
    elif product == 'Премиальная карта':
        return f"{name}, у вас стабильно крупный остаток на счете. Премиальная карта даст повышенный кешбэк до 4% и бесплатные снятия в банкоматах. Подключите сейчас."

    # --- Template for Кредитная карта ---
    elif product == 'Кредитная карта':
        if not transactions.empty:
            transactions['amount_kzt'] = transactions.apply(lambda row: row['amount'] * FX_RATES.get(row['currency'], 1), axis=1)
            top_cats = transactions.groupby('category')['amount_kzt'].sum().nlargest(3).index.tolist()
            if len(top_cats) >= 3:
                cat1, cat2, cat3 = top_cats[0], top_cats[1], top_cats[2]
                return f"{name}, ваши топ-категории — {cat1.lower()}, {cat2.lower()} и {cat3.lower()}. Кредитка даёт до {int(CREDIT_CARD_CASHBACK_RATE*100)}% кешбэка в любимых категориях. Оформить карту."
        return f"{name}, получайте до {int(CREDIT_CARD_CASHBACK_RATE*100)}% кешбэка в любимых категориях и на онлайн-сервисы с нашей кредитной картой. Оформить карту."

    # --- Template for FX/мультивалютный продукт ---
    elif product in ['Обмен валют', 'Депозит мультивалютный']:
        used_currencies = transactions[transactions['currency'] != 'KZT']['currency'].unique()
        if len(used_currencies) > 0:
            top_curr = used_currencies[0]
        else:
            top_curr = 'валюте'
        return f"{name}, заметили, вы часто платите в {top_curr}. В приложении выгодный обмен и автопокупка по целевому курсу. Настроить обмен."

    # --- Template for Вклады (сберегательный/накопительный) ---
    elif product in ['Депозит сберегательный', 'Депозит накопительный']:
        return f"{name}, у вас остаются свободные средства. Разместите их на вкладе — это удобно, чтобы копить и получать вознаграждение. Открыть вклад."

    # --- Template for Инвестиции ---
    elif product == 'Инвестиции':
        return f"{name}, попробуйте инвестиции с низким порогом входа и без комиссий на старте. Это простой способ заставить деньги работать. Открыть счёт."

    # --- Template for Кредит наличными ---
    elif product == 'Кредит наличными':
        return f"{name}, если нужен запас средств на крупные траты — можно оформить кредит наличными с гибкими выплатами. Узнать доступный лимит."

    # --- Template for Золотые слитки ---
    elif product == 'Золотые слитки':
         return f"{name}, диверсифицируйте свои сбережения. Покупка золотых слитков — простой способ защитить накопления. Посмотреть условия."

    else:
        return f"{name}, у нас есть для вас специальное предложение. Узнайте больше в приложении."


if __name__ == '__main__':
    analyze_clients()
