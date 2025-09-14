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
        
        existing_products = []
        if 'product' in client_trans.columns:
            existing_products.extend(client_trans['product'].unique())
        analysis = {
            'client_code': client_code,
            'name': client_info.get('name', f'Клиент_{client_code}'),
            'status': client_info.get('status', 'Стандартный клиент'),
            'age': client_info.get('age', 35),
            'city': client_info.get('city', 'Алматы'),
            'avg_balance': client_info.get('avg_monthly_balance_KZT', 100000),
            'transaction_stats': self._analyze_transactions(client_trans),
            'transfer_stats': self._analyze_transfers(client_transfers),
            'fx_activity': self._analyze_fx_activity(client_trans, client_transfers),
            'total_trans_amount': client_trans['amount'].sum(),
            'total_transf_amount': client_transfers['amount'].sum()
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

        # Monthly cashflow analysis
        monthly_cashflow = {}
        if 'date' in transfers_df.columns:
            transfers_df['month'] = transfers_df['date'].dt.to_period('M')
            monthly_income = transfers_df[transfers_df['direction'] == 'in'].groupby('month')['amount'].sum()
            monthly_expenses = transfers_df[transfers_df['direction'] == 'out'].groupby('month')['amount'].sum()
            monthly_net = monthly_income.subtract(monthly_expenses, fill_value=0)
            monthly_cashflow = monthly_net.to_dict()


        return {
            'total_income': income,
            'total_expenses': expenses,
            'net_cashflow': income - expenses,
            'atm_count': atm_count,
            'atm_amount': atm_amount,
            'loan_payments': loan_payments,
            'has_installments': loan_payments > 0,
            'p2p_count': p2p_count,
            'monthly_cashflow': monthly_cashflow
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
        """Calculate scores for each product based on the new logic"""
        scores = {}

        # Premium Card
        premium_score = (analysis['avg_balance'] / 2000000 * 50) + (analysis['total_transf_amount'] / 15000000 * 50)
        if analysis['status'] in ['Премиальный клиент', 'Зарплатный клиент']:
            premium_score += 10
        scores['premium_card'] = min(premium_score, 100)

        # Credit Card
        credit_score = (1 - analysis['avg_balance'] / 2000000 * 50) + (1 - analysis['age'] / 60 * 50)
        if analysis['status'] in ['Студент', 'Стандартный клиент']:
            credit_score += 10
        scores['credit_card'] = min(credit_score, 100)

        # Investments
        invest_score = (analysis['avg_balance'] / 2000000 * 40) + (analysis['total_trans_amount'] / 10000000 * 60)
        scores['investments'] = min(invest_score, 100)

        # Other products (using previous signal-based logic, adjusted)
        travel_spend = analysis['transaction_stats'].get('travel_spend', 0)
        total_spend = analysis['transaction_stats'].get('total_spend', 1)
        travel_ratio = travel_spend / total_spend
        scores['travel_card'] = min(travel_ratio * 200, 70) + (30 if analysis['fx_activity']['has_fx_activity'] else 0)

        fx_volume = analysis['fx_activity'].get('fx_volume', 0)
        scores['fx_exchange'] = min(fx_volume / 500000 * 70, 70) + (30 if analysis['fx_activity']['fx_spend'] > 0 else 0)

        loan_score = 0
        if analysis['transfer_stats'].get('net_cashflow', 0) < -100000: loan_score += 40
        if analysis['avg_balance'] < 100000: loan_score += 30
        if analysis['transfer_stats'].get('loan_payments', 0) > 0: loan_score += 30
        scores['cash_loan'] = loan_score

        multi_deposit_score = 0
        if analysis['avg_balance'] > 200000 and analysis['fx_activity']['has_fx_activity']:
            multi_deposit_score = min(analysis['avg_balance'] / 1000000 * 60, 60) + 40
        scores['multi_deposit'] = multi_deposit_score

        savings_deposit_score = 0
        if analysis['avg_balance'] > 500000:
             savings_deposit_score = min(analysis['avg_balance'] / 3000000 * 80, 80)
        if analysis['transaction_stats'].get('spending_volatility', float('inf')) < 100000:
            savings_deposit_score += 20
        scores['savings_deposit'] = savings_deposit_score

        accumulative_score = 0
        if len([flow for flow in analysis['transfer_stats'].get('monthly_cashflow', {}).values() if flow > 0]) >= 2:
            accumulative_score += 60
        if analysis['status'] == 'Студент':
            accumulative_score += 20
        scores['accumulative_deposit'] = accumulative_score

        scores['gold'] = min(analysis['avg_balance'] / 4000000 * 100, 100) if analysis['avg_balance'] > 1500000 else 0

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
    
    def generate_push_notification(self, analysis, product, scores):
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
        return generator(analysis, scores)
    
    def _format_amount(self, amount):
        """Format amount with spaces for thousands"""
        return f"{int(amount):,}".replace(',', ' ')

    def _get_russian_month(self, month_period):
        """Convert month period to Russian month name"""
        month_map = {
            1: 'январе', 2: 'феврале', 3: 'марте', 4: 'апреле',
            5: 'мае', 6: 'июне', 7: 'июле', 8: 'августе',
            9: 'сентябре', 10: 'октябре', 11: 'ноябре', 12: 'декабре'
        }
        return month_map.get(month_period.month)
    
    def _generate_travel_push(self, analysis, scores):
        name = analysis['name']
        travel_spend = analysis['transaction_stats'].get('travel_spend', 0)
        if travel_spend > 10000:
            return f"{name}, вы много тратите на поездки. С картой для путешествий вы могли бы получать до 4% кешбэка. Оформить карту."
        else:
            return f"{name}, планируете путешествие? Наша карта для путешествий даст кешбэк до 4% на отели и билеты. Узнать больше."

    def _generate_premium_push(self, analysis, scores):
        name = analysis['name']
        if analysis['avg_balance'] > 1000000:
            return f"{name}, ваш статус открывает премиум-привилегии. Кешбэк до 4% и бесплатное обслуживание. Оформить."
        else:
            return f"{name}, премиальная карта даст больше возможностей и повышенный кешбэк в любимых категориях. Узнать условия."

    def _generate_credit_push(self, analysis, scores):
        name = analysis['name']
        top_cats = analysis['transaction_stats'].get('top_categories', [])
        cat_str = ', '.join(top_cats[:2])
        return f"{name}, ваши топ-категории — {cat_str}. Кредитная карта даёт до 10% в любимых категориях и на онлайн-сервисы. Оформить карту."

    def _generate_fx_push(self, analysis, scores):
        name = analysis['name']
        curr = analysis['fx_activity'].get('primary_fx_currency', 'валюте')
        return f"{name}, вы часто платите в {curr}. В приложении выгодный обмен и авто-покупка по целевому курсу. Настроить обмен."

    def _generate_loan_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, если нужен запас на крупные траты — можно оформить кредит наличными с гибкими выплатами. Узнать доступный лимит."

    def _generate_multi_deposit_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, у вас остаются свободные средства и вы активны в валюте. Разместите их на мультивалютном вкладе. Открыть вклад."

    def _generate_savings_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, у вас остаются свободные средства. Разместите их на вкладе — удобно копить и получать вознаграждение. Открыть вклад."

    def _generate_accumulative_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, откладывайте регулярно и получайте проценты. Накопительный вклад ждёт. Начать копить."

    def _generate_investment_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, попробуйте инвестиции с низким порогом входа и без комиссий на старт. Открыть счёт."

    def _generate_gold_push(self, analysis, scores):
        name = analysis['name']
        return f"{name}, золотые слитки — надёжная защита капитала от инфляции. Узнать условия."
    
    def _generate_default_push(self, analysis, scores):
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
                push_text = self.generate_push_notification(analysis, best_product, scores)
                
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