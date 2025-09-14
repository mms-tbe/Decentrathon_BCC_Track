#!/usr/bin/env python3
"""
BCC Bank Personalized Push Notification Generator
Processes multiple client files from folders to generate personalized product recommendations
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class BCCPushNotificationGenerator:
    def __init__(self, data_dir="data"):
        """
        Initialize the generator with data directory
        
        Args:
            data_dir: Directory containing client data folders
        """
        self.data_dir = Path(data_dir)
        self.clients_df = None
        self.transactions_df = None
        self.transfers_df = None
        self.recommendations = []
        
        # Product scoring weights
        self.product_weights = {
            'travel_card': {'travel': 0.3, 'taxi': 0.2, 'fx': 0.2, 'hotels': 0.3},
            'premium_card': {'balance': 0.3, 'restaurants': 0.2, 'jewelry': 0.15, 'cosmetics': 0.15, 'atm': 0.2},
            'credit_card': {'top_categories': 0.3, 'online': 0.2, 'installments': 0.2, 'variety': 0.3},
            'fx_exchange': {'fx_activity': 0.5, 'foreign_spend': 0.5},
            'cash_loan': {'cash_gap': 0.4, 'low_balance': 0.3, 'loan_history': 0.3},
            'multi_deposit': {'free_balance': 0.3, 'fx_activity': 0.3, 'foreign_spend': 0.4},
            'savings_deposit': {'stable_balance': 0.4, 'low_volatility': 0.6},
            'accumulative_deposit': {'regular_surplus': 0.5, 'periodic_topups': 0.5},
            'investments': {'free_money': 0.4, 'financial_literacy': 0.3, 'age_factor': 0.3},
            'gold': {'high_liquidity': 0.5, 'diversification': 0.5}
        }
        
    def load_data_from_folders(self):
        """Load data from multiple CSV files in folders"""
        print("Loading data from folders...")
        
        # Define expected file patterns
        client_patterns = ['clients*.csv', 'client_profiles*.csv', 'profiles*.csv']
        transaction_patterns = ['transactions*.csv', 'trans*.csv']
        transfer_patterns = ['transfers*.csv', 'transfer*.csv']
        
        # Load client profiles
        self.clients_df = self._load_files_by_pattern(client_patterns, "clients")
        
        # Load transactions
        self.transactions_df = self._load_files_by_pattern(transaction_patterns, "transactions")
        
        # Load transfers
        self.transfers_df = self._load_files_by_pattern(transfer_patterns, "transfers")
        
        # Validate data
        self._validate_data()
        
        print(f"Loaded {len(self.clients_df)} clients")
        print(f"Loaded {len(self.transactions_df)} transactions")
        print(f"Loaded {len(self.transfers_df)} transfers")
        
    def _load_files_by_pattern(self, patterns, file_type):
        """Load and concatenate files matching patterns"""
        dfs = []
        
        # Search in main directory and subdirectories
        for pattern in patterns:
            # Search in main directory
            for file_path in self.data_dir.glob(pattern):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    dfs.append(df)
                    print(f"  Loaded {file_type}: {file_path.name}")
                except Exception as e:
                    try:
                        # Try with different encoding
                        df = pd.read_csv(file_path, encoding='cp1251')
                        dfs.append(df)
                        print(f"  Loaded {file_type}: {file_path.name} (cp1251)")
                    except:
                        print(f"  Error loading {file_path}: {e}")
            
            # Search in subdirectories
            for file_path in self.data_dir.rglob(pattern):
                if file_path not in self.data_dir.glob(pattern):  # Avoid duplicates
                    try:
                        df = pd.read_csv(file_path, encoding='utf-8')
                        dfs.append(df)
                        print(f"  Loaded {file_type}: {file_path}")
                    except Exception as e:
                        try:
                            df = pd.read_csv(file_path, encoding='cp1251')
                            dfs.append(df)
                            print(f"  Loaded {file_type}: {file_path} (cp1251)")
                        except:
                            print(f"  Error loading {file_path}: {e}")
        
        if not dfs:
            # Create sample data if no files found
            print(f"  No {file_type} files found. Creating sample data...")
            return self._create_sample_data(file_type)
        
        # Concatenate all dataframes
        result_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates if client_code exists
        if 'client_code' in result_df.columns:
            original_len = len(result_df)
            result_df = result_df.drop_duplicates()
            if original_len != len(result_df):
                print(f"  Removed {original_len - len(result_df)} duplicate {file_type} records")
        
        return result_df
    
    def _create_sample_data(self, file_type):
        """Create sample data for testing if files not found"""
        np.random.seed(42)  # For reproducibility
        
        if file_type == "clients":
            names = ['Айдар', 'Асель', 'Бауржан', 'Гульнара', 'Данияр', 
                    'Жанна', 'Ерлан', 'Камила', 'Нурлан', 'Сауле',
                    'Рамазан', 'Алия', 'Тимур', 'Мадина', 'Арман']
            
            data = []
            for i in range(1, 61):
                data.append({
                    'client_code': i,
                    'name': f'{np.random.choice(names)}',
                    'status': np.random.choice(['Студент', 'Зарплатный клиент', 'Премиальный клиент', 'Стандартный клиент']),
                    'age': np.random.randint(18, 65),
                    'city': np.random.choice(['Алматы', 'Астана', 'Шымкент', 'Караганда']),
                    'avg_monthly_balance_KZT': np.random.uniform(50000, 2000000)
                })
            return pd.DataFrame(data)
        
        elif file_type == "transactions":
            categories = ['Продукты питания', 'Кафе и рестораны', 'Такси', 'АЗС', 'Одежда и обувь', 
                         'Путешествия', 'Отели', 'Медицина', 'Спорт', 'Развлечения', 'Кино',
                         'Питомцы', 'Книги', 'Цветы', 'Едим дома', 'Смотрим дома', 'Играем дома',
                         'Косметика и Парфюмерия', 'Подарки', 'Ремонт дома', 'Мебель', 
                         'Спа и массаж', 'Ювелирные украшения']
            
            trans_list = []
            base_date = pd.Timestamp('2024-10-01')
            
            for client in range(1, 61):
                n_trans = np.random.randint(20, 100)
                for _ in range(n_trans):
                    trans_list.append({
                        'client_code': client,
                        'date': base_date + timedelta(days=np.random.randint(0, 90)),
                        'category': np.random.choice(categories),
                        'amount': np.random.exponential(10000) + 500,
                        'currency': np.random.choice(['KZT', 'USD', 'EUR'], p=[0.8, 0.15, 0.05])
                    })
            return pd.DataFrame(trans_list)
        
        else:  # transfers
            types = ['salary_in', 'stipend_in', 'family_in', 'cashback_in', 'refund_in', 
                    'card_in', 'p2p_out', 'card_out', 'atm_withdrawal', 'utilities_out',
                    'loan_payment_out', 'cc_repayment_out', 'installment_payment_out',
                    'fx_buy', 'fx_sell', 'invest_out', 'invest_in', 'deposit_topup_out',
                    'deposit_withdraw_in', 'gold_buy_out', 'gold_sell_in']
            
            transfer_list = []
            base_date = pd.Timestamp('2024-10-01')
            
            for client in range(1, 61):
                n_transfers = np.random.randint(10, 50)
                for _ in range(n_transfers):
                    transfer_type = np.random.choice(types)
                    transfer_list.append({
                        'client_code': client,
                        'date': base_date + timedelta(days=np.random.randint(0, 90)),
                        'type': transfer_type,
                        'direction': 'in' if '_in' in transfer_type else 'out',
                        'amount': np.random.exponential(20000) + 1000,
                        'currency': 'KZT'
                    })
            return pd.DataFrame(transfer_list)
    
    def _validate_data(self):
        """Validate loaded data"""
        # Ensure date columns are datetime
        if 'date' in self.transactions_df.columns:
            self.transactions_df['date'] = pd.to_datetime(self.transactions_df['date'], errors='coerce')
        if 'date' in self.transfers_df.columns:
            self.transfers_df['date'] = pd.to_datetime(self.transfers_df['date'], errors='coerce')
        
        # Ensure numeric columns
        if 'amount' in self.transactions_df.columns:
            self.transactions_df['amount'] = pd.to_numeric(self.transactions_df['amount'], errors='coerce')
        if 'amount' in self.transfers_df.columns:
            self.transfers_df['amount'] = pd.to_numeric(self.transfers_df['amount'], errors='coerce')
        if 'avg_monthly_balance_KZT' in self.clients_df.columns:
            self.clients_df['avg_monthly_balance_KZT'] = pd.to_numeric(
                self.clients_df['avg_monthly_balance_KZT'], errors='coerce'
            )
        
        # Fill NaN values
        self.transactions_df['amount'].fillna(0, inplace=True)
        self.transfers_df['amount'].fillna(0, inplace=True)
        self.clients_df['avg_monthly_balance_KZT'].fillna(100000, inplace=True)
    
    def analyze_client(self, client_code):
        """Analyze individual client behavior"""
        client_info = self.clients_df[self.clients_df['client_code'] == client_code]
        
        if client_info.empty:
            # Create default client if not found
            client_info = pd.DataFrame([{
                'client_code': client_code,
                'name': f'Клиент_{client_code}',
                'status': 'Стандартный клиент',
                'age': 35,
                'city': 'Алматы',
                'avg_monthly_balance_KZT': 100000
            }])
        
        client_info = client_info.iloc[0]
        client_trans = self.transactions_df[self.transactions_df['client_code'] == client_code]
        client_transfers = self.transfers_df[self.transfers_df['client_code'] == client_code]
        
        analysis = {
            'client_code': client_code,
            'name': client_info.get('name', f'Клиент_{client_code}'),
            'status': client_info.get('status', 'Стандартный клиент'),
            'age': client_info.get('age', 35),
            'city': client_info.get('city', 'Алматы'),
            'avg_balance': client_info.get('avg_monthly_balance_KZT', 100000),
            'transaction_stats': self._analyze_transactions(client_trans),
            'transfer_stats': self._analyze_transfers(client_transfers),
            'fx_activity': self._analyze_fx_activity(client_trans, client_transfers)
        }
        
        return analysis
    
    def _analyze_transactions(self, trans_df):
        """Analyze transaction patterns"""
        if trans_df.empty:
            return {}
        
        # Category spending
        category_spend = trans_df.groupby('category')['amount'].sum().to_dict()
        
        # Top categories
        top_categories = trans_df.groupby('category')['amount'].sum().nlargest(3).index.tolist()
        
        # Online services
        online_categories = ['Едим дома', 'Смотрим дома', 'Играем дома']
        online_spend = trans_df[trans_df['category'].isin(online_categories)]['amount'].sum()
        
        # Travel related
        travel_categories = ['Путешествия', 'Отели', 'Такси']
        travel_spend = trans_df[trans_df['category'].isin(travel_categories)]['amount'].sum()
        
        # Premium categories
        premium_categories = ['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны', 'Спа и массаж']
        premium_spend = trans_df[trans_df['category'].isin(premium_categories)]['amount'].sum()
        
        return {
            'total_spend': trans_df['amount'].sum(),
            'avg_transaction': trans_df['amount'].mean(),
            'transaction_count': len(trans_df),
            'category_spend': category_spend,
            'top_categories': top_categories,
            'online_spend': online_spend,
            'travel_spend': travel_spend,
            'premium_spend': premium_spend,
            'spending_volatility': trans_df['amount'].std() if len(trans_df) > 1 else 0
        }
    
    def _analyze_transfers(self, transfers_df):
        """Analyze transfer patterns"""
        if transfers_df.empty:
            return {}
        
        # Income vs expenses
        income = transfers_df[transfers_df['direction'] == 'in']['amount'].sum()
        expenses = transfers_df[transfers_df['direction'] == 'out']['amount'].sum()
        
        # ATM withdrawals
        atm_count = len(transfers_df[transfers_df['type'] == 'atm_withdrawal'])
        atm_amount = transfers_df[transfers_df['type'] == 'atm_withdrawal']['amount'].sum()
        
        # Loan/credit activity
        loan_types = ['loan_payment_out', 'cc_repayment_out', 'installment_payment_out']
        loan_transfers = transfers_df[transfers_df['type'].isin(loan_types)]
        loan_payments = loan_transfers['amount'].sum()
        
        # P2P transfers
        p2p_count = len(transfers_df[transfers_df['type'] == 'p2p_out'])
        
        return {
            'total_income': income,
            'total_expenses': expenses,
            'net_cashflow': income - expenses,
            'atm_count': atm_count,
            'atm_amount': atm_amount,
            'loan_payments': loan_payments,
            'has_installments': loan_payments > 0,
            'p2p_count': p2p_count
        }
    
    def _analyze_fx_activity(self, trans_df, transfers_df):
        """Analyze foreign exchange activity"""
        fx_currencies = ['USD', 'EUR']
        
        # FX transactions
        fx_trans = pd.DataFrame()
        if 'currency' in trans_df.columns:
            fx_trans = trans_df[trans_df['currency'].isin(fx_currencies)]
        
        fx_spend = fx_trans['amount'].sum() if not fx_trans.empty else 0
        
        # FX transfers
        fx_types = ['fx_buy', 'fx_sell', 'deposit_fx_topup_out', 'deposit_fx_withdraw_in']
        fx_transfers = pd.DataFrame()
        if not transfers_df.empty and 'type' in transfers_df.columns:
            fx_transfers = transfers_df[transfers_df['type'].isin(fx_types)]
        
        fx_volume = fx_transfers['amount'].sum() if not fx_transfers.empty else 0
        
        # Primary FX currency
        primary_fx = None
        if not fx_trans.empty and 'currency' in fx_trans.columns:
            currency_counts = fx_trans['currency'].value_counts()
            if len(currency_counts) > 0:
                primary_fx = currency_counts.index[0]
        
        return {
            'fx_spend': fx_spend,
            'fx_volume': fx_volume,
            'has_fx_activity': (fx_spend + fx_volume) > 0,
            'primary_fx_currency': primary_fx
        }
    
    def calculate_product_scores(self, analysis):
        """Calculate scores for each product"""
        scores = {}
        
        # Travel Card
        travel_score = 0
        if analysis['transaction_stats']:
            travel_spend = analysis['transaction_stats'].get('travel_spend', 0)
            total_spend = analysis['transaction_stats'].get('total_spend', 1)
            travel_ratio = (travel_spend / total_spend) if total_spend > 0 else 0
            travel_score = travel_ratio * 100
            
            # Bonus for FX activity
            if analysis['fx_activity']['has_fx_activity']:
                travel_score += 20
            
            # Bonus for frequent taxi use
            if 'category_spend' in analysis['transaction_stats']:
                taxi_spend = analysis['transaction_stats']['category_spend'].get('Такси', 0)
                if taxi_spend > 20000:
                    travel_score += 15
        
        scores['travel_card'] = min(travel_score, 100)
        
        # Premium Card
        premium_score = 0
        if analysis['avg_balance'] > 500000:
            premium_score += 40
        if analysis['avg_balance'] > 1000000:
            premium_score += 20
        
        if analysis['transaction_stats']:
            premium_spend = analysis['transaction_stats'].get('premium_spend', 0)
            total_spend = analysis['transaction_stats'].get('total_spend', 1)
            premium_ratio = (premium_spend / total_spend) if total_spend > 0 else 0
            premium_score += premium_ratio * 30
        
        if analysis['transfer_stats']:
            if analysis['transfer_stats'].get('atm_count', 0) > 5:
                premium_score += 15
            if analysis['transfer_stats'].get('p2p_count', 0) > 10:
                premium_score += 10
        
        scores['premium_card'] = min(premium_score, 100)
        
        # Credit Card
        credit_score = 0
        if analysis['transaction_stats']:
            # Diverse spending categories
            if len(analysis['transaction_stats'].get('top_categories', [])) >= 3:
                credit_score += 30
            
            # Online services usage
            online_spend = analysis['transaction_stats'].get('online_spend', 0)
            total_spend = analysis['transaction_stats'].get('total_spend', 1)
            online_ratio = (online_spend / total_spend) if total_spend > 0 else 0
            credit_score += online_ratio * 40
            
            # Transaction frequency
            if analysis['transaction_stats'].get('transaction_count', 0) > 50:
                credit_score += 15
        
        if analysis['transfer_stats'] and analysis['transfer_stats'].get('has_installments', False):
            credit_score += 15
        
        scores['credit_card'] = min(credit_score, 100)
        
        # FX Exchange
        fx_score = 0
        if analysis['fx_activity']['has_fx_activity']:
            fx_score = 50
            if analysis['fx_activity']['fx_volume'] > 100000:
                fx_score = 70
            if analysis['fx_activity']['fx_volume'] > 500000:
                fx_score = 90
        scores['fx_exchange'] = fx_score
        
        # Cash Loan
        loan_score = 0
        if analysis['transfer_stats']:
            # Negative cash flow
            if analysis['transfer_stats'].get('net_cashflow', 0) < -50000:
                loan_score += 40
            
            # Low balance
            if analysis['avg_balance'] < 100000:
                loan_score += 30
            
            # Existing loan payments
            if analysis['transfer_stats'].get('loan_payments', 0) > 0:
                loan_score += 30
        scores['cash_loan'] = min(loan_score, 100)
        
        # Deposits
        deposit_base_score = 0
        if analysis['avg_balance'] > 200000:
            deposit_base_score = 40
        if analysis['avg_balance'] > 500000:
            deposit_base_score = 60
        if analysis['avg_balance'] > 1000000:
            deposit_base_score = 80
        
        # Multi-currency deposit
        scores['multi_deposit'] = deposit_base_score if analysis['fx_activity']['has_fx_activity'] else 0
        
        # Savings deposit
        low_volatility = False
        if analysis['transaction_stats']:
            volatility = analysis['transaction_stats'].get('spending_volatility', float('inf'))
            low_volatility = volatility < 50000
        scores['savings_deposit'] = deposit_base_score if low_volatility else deposit_base_score * 0.7
        
        # Accumulative deposit
        scores['accumulative_deposit'] = deposit_base_score * 0.8 if analysis['avg_balance'] > 100000 else 0
        
        # Investments
        invest_score = 0
        if analysis['avg_balance'] > 500000:
            invest_score = 50
            if analysis['age'] < 45:
                invest_score += 20
            if analysis['age'] < 35:
                invest_score += 10
        scores['investments'] = invest_score
        
        # Gold
        gold_score = 0
        if analysis['avg_balance'] > 1000000:
            gold_score = 60
        if analysis['avg_balance'] > 2000000:
            gold_score = 80
        scores['gold'] = gold_score
        
        return scores
    
    def select_best_product(self, scores):
        """Select the best product based on scores"""
        if not scores:
            return 'savings_deposit'
        
        # Sort products by score
        sorted_products = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # Return the best scoring product with minimum threshold
        if sorted_products[0][1] > 20:
            return sorted_products[0][0]
        else:
            return 'savings_deposit'
    
    def generate_push_notification(self, analysis, product):
        """Generate personalized push notification"""
        templates = {
            'travel_card': self._generate_travel_push,
            'premium_card': self._generate_premium_push,
            'credit_card': self._generate_credit_push,
            'fx_exchange': self._generate_fx_push,
            'cash_loan': self._generate_loan_push,
            'multi_deposit': self._generate_multi_deposit_push,
            'savings_deposit': self._generate_savings_push,
            'accumulative_deposit': self._generate_accumulative_push,
            'investments': self._generate_investment_push,
            'gold': self._generate_gold_push
        }
        
        generator = templates.get(product, self._generate_default_push)
        return generator(analysis)
    
    def _format_amount(self, amount):
        """Format amount with spaces for thousands"""
        return f"{int(amount):,}".replace(',', ' ')
    
    def _generate_travel_push(self, analysis):
        name = analysis['name']
        travel_spend = 0
        taxi_spend = 0
        
        if analysis['transaction_stats']:
            travel_spend = analysis['transaction_stats'].get('travel_spend', 0)
            if 'category_spend' in analysis['transaction_stats']:
                taxi_spend = analysis['transaction_stats']['category_spend'].get('Такси', 0)
        
        total_travel = travel_spend + taxi_spend
        
        if total_travel > 100000:
            cashback = int(total_travel * 0.04)
            return f"{name}, за 3 месяца {self._format_amount(total_travel)} ₸ на поездки. Карта для путешествий вернёт {self._format_amount(cashback)} ₸. Оформить карту."
        elif analysis['fx_activity']['has_fx_activity']:
            curr = analysis['fx_activity'].get('primary_fx_currency', 'USD')
            return f"{name}, вы часто платите в {curr}. Тревел-карта даст выгодный курс и кешбэк. Открыть карту."
        elif taxi_spend > 20000:
            return f"{name}, {self._format_amount(taxi_spend)} ₸ на такси за месяц. С картой для путешествий вернётся 4%. Получить карту."
        else:
            return f"{name}, планируете путешествие? Карта даст кешбэк до 4% на отели и билеты. Узнать больше."
    
    def _generate_premium_push(self, analysis):
        name = analysis['name']
        balance = analysis['avg_balance']
        
        if balance > 1500000:
            return f"{name}, ваш статус открывает премиум-привилегии. Кешбэк до 4% и бесплатное обслуживание. Оформить."
        elif balance > 1000000:
            return f"{name}, с балансом {self._format_amount(balance)} ₸ доступна премиум-карта. Повышенный кешбэк ждёт. Подключить."
        else:
            premium_spend = 0
            if analysis['transaction_stats']:
                premium_spend = analysis['transaction_stats'].get('premium_spend', 0)
            
            if premium_spend > 50000:
                return f"{name}, траты в премиум-категориях {self._format_amount(premium_spend)} ₸. Удвойте кешбэк премиум-картой. Получить."
            else:
                return f"{name}, премиальная карта даст больше возможностей и привилегий. Узнать условия."
    
    def _generate_credit_push(self, analysis):
        name = analysis['name']
        
        if analysis['transaction_stats'] and analysis['transaction_stats'].get('top_categories'):
            cats = analysis['transaction_stats']['top_categories'][:2]
            cat_str = ' и '.join([cat for cat in cats if len(cat) < 20])  # Shorten long categories
            if cat_str:
                return f"{name}, любимые категории — {cat_str}. Кредитная карта даст до 10% кешбэка там. Оформить."
        
        online_spend = 0
        if analysis['transaction_stats']:
            online_spend = analysis['transaction_stats'].get('online_spend', 0)
        
        if online_spend > 30000:
            return f"{name}, {self._format_amount(online_spend)} ₸ на онлайн-сервисы. Кредитная карта вернёт 10%. Получить карту."
        else:
            return f"{name}, кредитная карта с льготным периодом 60 дней и кешбэком до 10%. Узнать лимит."
    
    def _generate_fx_push(self, analysis):
        name = analysis['name']
        curr = analysis['fx_activity'].get('primary_fx_currency', 'валюте')
        fx_volume = analysis['fx_activity'].get('fx_volume', 0)
        
        if fx_volume > 100000:
            return f"{name}, обмениваете {self._format_amount(fx_volume)} ₸ в {curr}. В приложении курс выгоднее. Настроить."
        else:
            return f"{name}, работаете с {curr}? Выгодный курс без скрытых комиссий в приложении. Попробовать."
    
    def _generate_loan_push(self, analysis):
        name = analysis['name']
        cashflow = 0
        if analysis['transfer_stats']:
            cashflow = analysis['transfer_stats'].get('net_cashflow', 0)
        
        if cashflow < -100000:
            return f"{name}, поможем с временным дефицитом средств. Кредит с удобными выплатами. Узнать лимит."
        else:
            return f"{name}, нужны средства на важные цели? Быстрое решение по кредиту. Подать заявку."
    
    def _generate_multi_deposit_push(self, analysis):
        name = analysis['name']
        balance = analysis['avg_balance']
        
        if balance > 1000000:
            return f"{name}, {self._format_amount(balance)} ₸ могут работать в разных валютах. Мультивалютный вклад. Открыть."
        else:
            return f"{name}, сохраните средства в разных валютах с выгодной ставкой. Открыть вклад."
    
    def _generate_savings_push(self, analysis):
        name = analysis['name']
        balance = analysis['avg_balance']
        
        if balance > 500000:
            return f"{name}, свободные {self._format_amount(balance)} ₸ могут приносить доход. Вклад с максимальной ставкой. Разместить."
        else:
            return f"{name}, начните копить с выгодом. Сберегательный вклад под высокий процент. Открыть вклад."
    
    def _generate_accumulative_push(self, analysis):
        name = analysis['name']
        balance = analysis['avg_balance']
        
        if balance > 200000:
            return f"{name}, откладывайте регулярно и получайте проценты. Накопительный вклад ждёт. Начать копить."
        else:
            return f"{name}, даже небольшие суммы растут на накопительном вкладе. Открыть счёт."
    
    def _generate_investment_push(self, analysis):
        name = analysis['name']
        age = analysis['age']
        
        if age < 35:
            return f"{name}, самое время начать инвестировать. Брокерский счёт без комиссии для новичков. Открыть."
        elif age < 45:
            return f"{name}, готовы к инвестициям? Низкий порог входа и поддержка на старте. Начать инвестировать."
        else:
            return f"{name}, диверсифицируйте накопления через инвестиции. Персональная консультация. Узнать больше."
    
    def _generate_gold_push(self, analysis):
        name = analysis['name']
        balance = analysis['avg_balance']
        
        if balance > 2000000:
            return f"{name}, с капиталом {self._format_amount(balance)} ₸ важна диверсификация. Золото защитит средства. Купить."
        else:
            return f"{name}, золотые слитки — надёжная защита капитала от инфляции. Узнать условия."
    
    def _generate_default_push(self, analysis):
        name = analysis['name']
        return f"{name}, у нас есть выгодное предложение специально для вас. Узнать подробности."
    
    def process_all_clients(self):
        """Process all clients and generate recommendations"""
        print("\nProcessing all clients...")
        
        # Get unique client codes
        all_client_codes = set()
        
        if self.clients_df is not None and 'client_code' in self.clients_df.columns:
            all_client_codes.update(self.clients_df['client_code'].unique())
        
        if self.transactions_df is not None and 'client_code' in self.transactions_df.columns:
            all_client_codes.update(self.transactions_df['client_code'].unique())
        
        if self.transfers_df is not None and 'client_code' in self.transfers_df.columns:
            all_client_codes.update(self.transfers_df['client_code'].unique())
        
        if not all_client_codes:
            print("No client codes found in data")
            return
        
        print(f"Found {len(all_client_codes)} unique clients to process")
        
        for i, client_code in enumerate(sorted(all_client_codes), 1):
            try:
                # Analyze client
                analysis = self.analyze_client(client_code)
                
                # Calculate product scores
                scores = self.calculate_product_scores(analysis)
                
                # Select best product
                best_product = self.select_best_product(scores)
                
                # Generate push notification
                push_text = self.generate_push_notification(analysis, best_product)
                
                # Store recommendation
                self.recommendations.append({
                    'client_code': int(client_code),
                    'product': self._get_product_name(best_product),
                    'push_notification': push_text
                })
                
                if i % 10 == 0:
                    print(f"  Processed {i}/{len(all_client_codes)} clients...")
                    
            except Exception as e:
                print(f"  Error processing client {client_code}: {e}")
                # Add default recommendation
                self.recommendations.append({
                    'client_code': int(client_code),
                    'product': 'Сберегательный депозит',
                    'push_notification': f"У нас есть выгодное предложение для вас. Узнать подробности."
                })
        
        print(f"Completed processing {len(self.recommendations)} clients")
    
    def _get_product_name(self, product_key):
        """Get Russian product name"""
        product_names = {
            'travel_card': 'Карта для путешествий',
            'premium_card': 'Премиальная карта',
            'credit_card': 'Кредитная карта',
            'fx_exchange': 'Обмен валют',
            'cash_loan': 'Кредит наличными',
            'multi_deposit': 'Депозит мультивалютный',
            'savings_deposit': 'Депозит сберегательный',
            'accumulative_deposit': 'Депозит накопительный',
            'investments': 'Инвестиции',
            'gold': 'Золотые слитки'
        }
        return product_names.get(product_key, 'Депозит сберегательный')
    
    def save_results(self, output_file='recommendations.csv'):
        """Save results to CSV file"""
        if not self.recommendations:
            print("No recommendations to save")
            return
        
        # Create DataFrame and sort by client_code
        df = pd.DataFrame(self.recommendations)
        df = df.sort_values('client_code')
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
        print(f"\nResults saved to {output_file}")
        print(f"Total recommendations: {len(df)}")
        
        # Print statistics
        print("\nProduct distribution:")
        product_counts = df['product'].value_counts()
        for product, count in product_counts.items():
            print(f"  {product}: {count} ({count*100/len(df):.1f}%)")
        
        # Print sample recommendations
        print("\nSample recommendations (first 3):")
        for _, row in df.head(3).iterrows():
            print(f"  Client {row['client_code']}: {row['product']}")
            print(f"    Push: {row['push_notification'][:80]}...")
    
    def run(self, output_file='recommendations.csv'):
        """Main execution method"""
        print("=" * 60)
        print("BCC Bank Push Notification Generator")
        print("=" * 60)
        
        # Load data
        self.load_data_from_folders()
        
        # Process clients
        self.process_all_clients()
        
        # Save results
        self.save_results(output_file)
        
        print("\nProcessing complete!")
        print("=" * 60)
        
        return self.recommendations


def main():
    """Main function with different usage modes"""
    import argparse
    
    parser = argparse.ArgumentParser(description='BCC Push Notification Generator')
    parser.add_argument('--data-dir', type=str, default='data', 
                       help='Directory containing client data files')
    parser.add_argument('--output', type=str, default='recommendations.csv',
                       help='Output CSV file name')
    parser.add_argument('--mode', type=str, default='auto',
                       choices=['auto', 'manual', 'batch'],
                       help='Processing mode: auto (find files automatically), manual (specify files), batch (process multiple folders)')
    parser.add_argument('--clients-file', type=str, help='Path to clients CSV file (manual mode)')
    parser.add_argument('--transactions-file', type=str, help='Path to transactions CSV file (manual mode)')
    parser.add_argument('--transfers-file', type=str, help='Path to transfers CSV file (manual mode)')
    parser.add_argument('--batch-dirs', nargs='+', help='List of directories to process (batch mode)')
    
    args = parser.parse_args()
    
    if args.mode == 'manual':
        # Manual mode - specify exact files
        generator = BCCPushNotificationGenerator()
        
        print("Manual mode: Loading specified files...")
        
        if args.clients_file:
            generator.clients_df = pd.read_csv(args.clients_file, encoding='utf-8')
            print(f"Loaded clients from {args.clients_file}")
        else:
            print("Warning: No clients file specified, will create sample data")
            generator.clients_df = generator._create_sample_data('clients')
        
        if args.transactions_file:
            generator.transactions_df = pd.read_csv(args.transactions_file, encoding='utf-8')
            print(f"Loaded transactions from {args.transactions_file}")
        else:
            print("Warning: No transactions file specified, will create sample data")
            generator.transactions_df = generator._create_sample_data('transactions')
        
        if args.transfers_file:
            generator.transfers_df = pd.read_csv(args.transfers_file, encoding='utf-8')
            print(f"Loaded transfers from {args.transfers_file}")
        else:
            print("Warning: No transfers file specified, will create sample data")
            generator.transfers_df = generator._create_sample_data('transfers')
        
        generator._validate_data()
        generator.process_all_clients()
        generator.save_results(args.output)
        
    elif args.mode == 'batch':
        # Batch mode - process multiple directories
        all_recommendations = []
        
        dirs_to_process = args.batch_dirs if args.batch_dirs else [args.data_dir]
        
        for dir_path in dirs_to_process:
            print(f"\n{'='*60}")
            print(f"Processing directory: {dir_path}")
            print('='*60)
            
            try:
                generator = BCCPushNotificationGenerator(data_dir=dir_path)
                recommendations = generator.run(f"{Path(dir_path).name}_recommendations.csv")
                all_recommendations.extend(recommendations)
            except Exception as e:
                print(f"Error processing directory {dir_path}: {e}")
        
        # Save combined results
        if all_recommendations:
            combined_df = pd.DataFrame(all_recommendations)
            combined_df = combined_df.sort_values('client_code')
            combined_df.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"\n{'='*60}")
            print(f"Combined results saved to {args.output}")
            print(f"Total recommendations: {len(combined_df)}")
            print('='*60)
    
    else:  # auto mode
        # Auto mode - automatically find and process files
        generator = BCCPushNotificationGenerator(data_dir=args.data_dir)
        generator.run(args.output)


# Example usage functions for different scenarios
def process_single_folder(folder_path='data'):
    """Process files from a single folder"""
    generator = BCCPushNotificationGenerator(data_dir=folder_path)
    return generator.run()


def process_multiple_folders(folder_list):
    """Process files from multiple folders and combine results"""
    all_recommendations = []
    
    for folder in folder_list:
        print(f"\n{'='*60}")
        print(f"Processing folder: {folder}")
        print('='*60)
        
        try:
            generator = BCCPushNotificationGenerator(data_dir=folder)
            recommendations = generator.run(f"{Path(folder).name}_recommendations.csv")
            all_recommendations.extend(recommendations)
        except Exception as e:
            print(f"Error processing folder {folder}: {e}")
    
    # Save combined results
    if all_recommendations:
        combined_df = pd.DataFrame(all_recommendations)
        combined_df = combined_df.sort_values('client_code')
        combined_df.to_csv('combined_recommendations.csv', index=False, encoding='utf-8-sig')
        print(f"\n{'='*60}")
        print("FINAL COMBINED RESULTS")
        print('='*60)
        print(f"Combined results saved to combined_recommendations.csv")
        print(f"Total recommendations: {len(combined_df)}")
        print("\nProduct distribution:")
        print(combined_df['product'].value_counts())
    
    return all_recommendations


def process_with_file_mapping(file_mapping):
    """
    Process with specific file mappings
    
    Args:
        file_mapping: dict with keys 'clients', 'transactions', 'transfers'
                     pointing to file paths
    """
    generator = BCCPushNotificationGenerator()
    
    print("Processing with specific file mapping...")
    
    # Load specific files
    if 'clients' in file_mapping:
        generator.clients_df = pd.read_csv(file_mapping['clients'], encoding='utf-8')
        print(f"Loaded clients from {file_mapping['clients']}")
    else:
        generator.clients_df = generator._create_sample_data('clients')
        print("No clients file specified, created sample data")
    
    if 'transactions' in file_mapping:
        generator.transactions_df = pd.read_csv(file_mapping['transactions'], encoding='utf-8')
        print(f"Loaded transactions from {file_mapping['transactions']}")
    else:
        generator.transactions_df = generator._create_sample_data('transactions')
        print("No transactions file specified, created sample data")
    
    if 'transfers' in file_mapping:
        generator.transfers_df = pd.read_csv(file_mapping['transfers'], encoding='utf-8')
        print(f"Loaded transfers from {file_mapping['transfers']}")
    else:
        generator.transfers_df = generator._create_sample_data('transfers')
        print("No transfers file specified, created sample data")
    
    generator._validate_data()
    generator.process_all_clients()
    generator.save_results()
    
    return generator.recommendations


if __name__ == "__main__":
    main()