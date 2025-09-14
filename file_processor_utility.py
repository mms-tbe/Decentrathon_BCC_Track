#!/usr/bin/env python3
"""
Utility script to organize and process BCC data files from various folder structures
Compatible with the Decentrathon_BCC_Track repository structure
"""

import os
import sys
import glob
import shutil
import pandas as pd
from pathlib import Path
import json
from typing import Dict, List, Optional

class BCCFileOrganizer:
    """
    Organizes and prepares BCC data files for processing
    """
    
    def __init__(self, repo_path: str = "."):
        self.repo_path = Path(repo_path)
        self.data_files = {
            'clients': [],
            'transactions': [],
            'transfers': []
        }
        self.organized_data_path = self.repo_path / "organized_data"
        
    def scan_repository(self):
        """
        Scan the repository for data files
        """
        print("Scanning repository for data files...")
        print(f"Repository path: {self.repo_path}")
        
        # Define patterns for each file type
        patterns = {
            'clients': [
                'clients.csv'
            ],
            'transactions': [
                'Transactions/*.csv'
            ],
            'transfers': [
                'Transfers/*.csv'
            ]
        }
        
        # Search for files
        for file_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                files = list(self.repo_path.glob(pattern))
                for file in files:
                    # Skip organized_data directory
                    if 'organized_data' not in str(file):
                        self.data_files[file_type].append(file)
                        print(f"  Found {file_type}: {file.relative_to(self.repo_path)}")
        
        # Report findings
        print("\nScan complete:")
        for file_type, files in self.data_files.items():
            print(f"  {file_type}: {len(files)} files")
            
        return self.data_files
    
    def validate_file_structure(self, file_path: Path, file_type: str) -> bool:
        """
        Validate that a CSV file has the expected structure
        """
        try:
            df = pd.read_csv(file_path, nrows=5)
            
            # Define expected columns for each file type
            expected_columns = {
                'clients': ['client_code'],  # Must have at least client_code
                'transactions': ['client_code', 'amount'],  # Must have these
                'transfers': ['client_code', 'amount']  # Must have these
            }
            
            required = expected_columns.get(file_type, [])
            has_required = all(col in df.columns for col in required)
            
            if not has_required:
                print(f"    Warning: {file_path.name} missing required columns: {required}")
                return False
                
            return True
            
        except Exception as e:
            print(f"    Error validating {file_path.name}: {e}")
            return False
    
    def organize_files(self, validate: bool = True):
        """
        Organize files into a structured directory
        """
        print("\nOrganizing files...")
        
        # Create organized directory
        self.organized_data_path.mkdir(exist_ok=True)
        
        organized = {
            'clients': [],
            'transactions': [],
            'transfers': []
        }
        
        for file_type, files in self.data_files.items():
            type_dir = self.organized_data_path / file_type
            type_dir.mkdir(exist_ok=True)
            
            for i, file in enumerate(files):
                if validate and not self.validate_file_structure(file, file_type):
                    continue
                    
                # Copy file to organized directory
                new_name = f"{file_type}_{i+1:03d}.csv"
                new_path = type_dir / new_name
                shutil.copy2(file, new_path)
                organized[file_type].append(new_path)
                print(f"  Copied {file.name} -> {new_path.relative_to(self.repo_path)}")
        
        print(f"\nFiles organized in: {self.organized_data_path}")
        return organized
    
    def merge_files_by_type(self):
        """
        Merge all files of the same type into single files
        """
        print("\nMerging files by type...")
        
        merged_dir = self.organized_data_path / "merged"
        merged_dir.mkdir(exist_ok=True)
        
        merged_files = {}
        
        for file_type, files in self.data_files.items():
            if not files:
                print(f"  No {file_type} files to merge")
                continue
                
            # Read and concatenate all files
            dfs = []
            for file in files:
                try:
                    df = pd.read_csv(file)
                    dfs.append(df)
                    print(f"  Read {file.name}: {len(df)} rows")
                except Exception as e:
                    print(f"  Error reading {file.name}: {e}")
            
            if dfs:
                # Merge dataframes
                merged_df = pd.concat(dfs, ignore_index=True)
                
                # Remove duplicates if client_code exists
                if 'client_code' in merged_df.columns:
                    original_len = len(merged_df)
                    merged_df = merged_df.drop_duplicates()
                    if original_len != len(merged_df):
                        print(f"  Removed {original_len - len(merged_df)} duplicate rows")
                
                # Save merged file
                output_file = merged_dir / f"{file_type}_merged.csv"
                merged_df.to_csv(output_file, index=False)
                merged_files[file_type] = output_file
                print(f"  Merged {file_type}: {len(merged_df)} total rows -> {output_file.name}")
        
        return merged_files
    
    def create_sample_data(self, output_dir: Optional[Path] = None):
        """
        Create sample data files for testing
        """
        import numpy as np
        from datetime import datetime, timedelta
        
        if output_dir is None:
            output_dir = self.repo_path / "sample_data"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        print(f"\nCreating sample data in {output_dir}...")
        
        # Create sample clients
        clients_data = []
        statuses = ['Студент', 'Зарплатный клиент', 'Премиальный клиент', 'Стандартный клиент']
        cities = ['Алматы', 'Астана', 'Шымкент', 'Караганда', 'Актобе', 'Тараз']
        names = ['Айдар', 'Асель', 'Бауржан', 'Гульнара', 'Данияр', 'Жанна', 'Ерлан', 'Камила', 'Нурлан', 'Сауле']
        
        for i in range(1, 61):
            clients_data.append({
                'client_code': i,
                'name': f"{np.random.choice(names)}_{i}",
                'status': np.random.choice(statuses),
                'age': np.random.randint(18, 65),
                'city': np.random.choice(cities),
                'avg_monthly_balance_KZT': np.random.uniform(50000, 2000000)
            })
        
        clients_df = pd.DataFrame(clients_data)
        clients_df.to_csv(output_dir / 'clients.csv', index=False)
        print(f"  Created clients.csv: {len(clients_df)} clients")
        
        # Create sample transactions
        categories = [
            'Продукты питания', 'Кафе и рестораны', 'Такси', 'АЗС', 
            'Одежда и обувь', 'Путешествия', 'Отели', 'Медицина', 
            'Спорт', 'Развлечения', 'Кино', 'Питомцы', 'Книги', 
            'Цветы', 'Едим дома', 'Смотрим дома', 'Играем дома', 
            'Косметика и Парфюмерия', 'Подарки', 'Ремонт дома', 
            'Мебель', 'Спа и массаж', 'Ювелирные украшения'
        ]
        
        transactions_data = []
        base_date = datetime(2024, 10, 1)
        
        for client_code in range(1, 61):
            # Generate 30-100 transactions per client
            n_transactions = np.random.randint(30, 100)
            
            for _ in range(n_transactions):
                transactions_data.append({
                    'client_code': client_code,
                    'date': base_date + timedelta(days=np.random.randint(0, 90)),
                    'category': np.random.choice(categories),
                    'amount': np.random.exponential(15000) + 500,  # Exponential distribution for amounts
                    'currency': np.random.choice(['KZT', 'USD', 'EUR'], p=[0.85, 0.10, 0.05])
                })
        
        transactions_df = pd.DataFrame(transactions_data)
        transactions_df.to_csv(output_dir / 'transactions.csv', index=False)
        print(f"  Created transactions.csv: {len(transactions_df)} transactions")
        
        # Create sample transfers
        transfer_types = [
            'salary_in', 'stipend_in', 'family_in', 'cashback_in', 
            'refund_in', 'card_in', 'p2p_out', 'card_out', 
            'atm_withdrawal', 'utilities_out', 'loan_payment_out', 
            'cc_repayment_out', 'installment_payment_out', 
            'fx_buy', 'fx_sell', 'invest_out', 'invest_in', 
            'deposit_topup_out', 'deposit_withdraw_in'
        ]
        
        transfers_data = []
        
        for client_code in range(1, 61):
            # Generate 10-50 transfers per client
            n_transfers = np.random.randint(10, 50)
            
            for _ in range(n_transfers):
                transfer_type = np.random.choice(transfer_types)
                direction = 'in' if '_in' in transfer_type else 'out'
                
                transfers_data.append({
                    'client_code': client_code,
                    'date': base_date + timedelta(days=np.random.randint(0, 90)),
                    'type': transfer_type,
                    'direction': direction,
                    'amount': np.random.exponential(30000) + 1000,
                    'currency': 'KZT' if 'fx' not in transfer_type else np.random.choice(['USD', 'EUR'])
                })
        
        transfers_df = pd.DataFrame(transfers_data)
        transfers_df.to_csv(output_dir / 'transfers.csv', index=False)
        print(f"  Created transfers.csv: {len(transfers_df)} transfers")
        
        return output_dir
    
    def generate_config(self):
        """
        Generate a configuration file for the processing pipeline
        """
        config = {
            'data_sources': {
                'clients': [str(f) for f in self.data_files['clients']],
                'transactions': [str(f) for f in self.data_files['transactions']],
                'transfers': [str(f) for f in self.data_files['transfers']]
            },
            'organized_path': str(self.organized_data_path),
            'processing_options': {
                'validate_data': True,
                'remove_duplicates': True,
                'handle_missing': 'fill_default'
            }
        }
        
        config_file = self.repo_path / 'data_config.json'
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2, default=str)
        
        print(f"\nConfiguration saved to: {config_file}")
        return config_file


def main():
    """
    Main function to run the file organizer
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='BCC Data File Organizer')
    parser.add_argument('--repo-path', type=str, default='.',
                       help='Path to the repository')
    parser.add_argument('--action', type=str, default='scan',
                       choices=['scan', 'organize', 'merge', 'sample', 'all'],
                       help='Action to perform')
    parser.add_argument('--output-dir', type=str,
                       help='Output directory for organized files')
    
    args = parser.parse_args()
    
    organizer = BCCFileOrganizer(repo_path=args.repo_path)
    
    if args.action in ['scan', 'all']:
        organizer.scan_repository()
    
    if args.action in ['organize', 'all']:
        organizer.organize_files()
    
    if args.action in ['merge', 'all']:
        merged_files = organizer.merge_files_by_type()
        if merged_files:
            print("\nMerged files ready for processing:")
            for file_type, path in merged_files.items():
                print(f"  {file_type}: {path}")
    
    if args.action == 'sample':
        sample_dir = organizer.create_sample_data(args.output_dir)
        print(f"\nSample data created in: {sample_dir}")
        print("\nTo process the sample data, run:")
        print(f"  python bcc_push_generator.py --data-dir {sample_dir}")
    
    if args.action == 'all':
        organizer.generate_config()
        print("\n" + "="*60)
        print("Setup complete! You can now run the push notification generator:")
        print(f"  python bcc_push_generator.py --data-dir {organizer.organized_data_path / 'merged'}")


if __name__ == "__main__":
    main()
    