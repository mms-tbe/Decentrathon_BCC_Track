#!/usr/bin/env python3
"""
Run script adapted for your specific folder structure:
- Transactions/ folder
- Transfers/ folder  
- clients.csv in root
"""

import os
import sys
import pandas as pd  # Add this import!
import numpy as np   # Add this import!
from pathlib import Path

def run_with_folder_structure():
    """
    Process files with your specific folder structure
    """
    # Import the generator (make sure bcc_push_generator.py is in the same directory)
    try:
        from bcc_push_generator import BCCPushNotificationGenerator
    except ImportError:
        print("ERROR: bcc_push_generator.py not found in the current directory!")
        print("Make sure bcc_push_generator.py is in the same folder as this script.")
        return
    
    print("=" * 60)
    print("BCC Push Notification Generator")
    print("Processing with your folder structure")
    print("=" * 60)
    
    # Initialize generator
    generator = BCCPushNotificationGenerator()
    
    # 1. Load clients.csv from root
    clients_file = Path('clients.csv')
    if clients_file.exists():
        print(f"\nLoading clients from: {clients_file}")
        try:
            generator.clients_df = pd.read_csv(clients_file, encoding='utf-8')
            print(f"  Loaded {len(generator.clients_df)} clients")
        except Exception as e:
            print(f"  Error loading clients.csv: {e}")
            try:
                # Try different encoding
                generator.clients_df = pd.read_csv(clients_file, encoding='cp1251')
                print(f"  Loaded {len(generator.clients_df)} clients (cp1251 encoding)")
            except:
                print("  Failed to load clients.csv")
                return
    else:
        print("ERROR: clients.csv not found in root directory!")
        print(f"Current directory: {os.getcwd()}")
        print(f"Files in directory: {list(Path('.').glob('*.csv'))}")
        return
    
    # 2. Load all CSV files from Transactions folder
    transactions_dir = Path('Transactions')
    if transactions_dir.exists():
        print(f"\nLoading transactions from: {transactions_dir}/")
        trans_files = list(transactions_dir.glob('*.csv'))
        
        if trans_files:
            trans_dfs = []
            for file in trans_files:
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                    trans_dfs.append(df)
                    print(f"  Loaded {file.name}: {len(df)} records")
                except Exception as e:
                    try:
                        df = pd.read_csv(file, encoding='cp1251')
                        trans_dfs.append(df)
                        print(f"  Loaded {file.name}: {len(df)} records (cp1251)")
                    except:
                        print(f"  Error loading {file.name}: {e}")
            
            if trans_dfs:
                generator.transactions_df = pd.concat(trans_dfs, ignore_index=True)
                print(f"  Total transactions: {len(generator.transactions_df)}")
            else:
                print("  Warning: No valid transaction files loaded")
                generator.transactions_df = pd.DataFrame()
        else:
            print("  No CSV files found in Transactions folder")
            generator.transactions_df = pd.DataFrame()
    else:
        print("WARNING: Transactions folder not found! Creating sample transactions...")
        generator.transactions_df = generator._create_sample_data('transactions')
    
    # 3. Load all CSV files from Transfers folder
    transfers_dir = Path('Transfers')
    if transfers_dir.exists():
        print(f"\nLoading transfers from: {transfers_dir}/")
        transfer_files = list(transfers_dir.glob('*.csv'))
        
        if transfer_files:
            transfer_dfs = []
            for file in transfer_files:
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                    transfer_dfs.append(df)
                    print(f"  Loaded {file.name}: {len(df)} records")
                except Exception as e:
                    try:
                        df = pd.read_csv(file, encoding='cp1251')
                        transfer_dfs.append(df)
                        print(f"  Loaded {file.name}: {len(df)} records (cp1251)")
                    except:
                        print(f"  Error loading {file.name}: {e}")
            
            if transfer_dfs:
                generator.transfers_df = pd.concat(transfer_dfs, ignore_index=True)
                print(f"  Total transfers: {len(generator.transfers_df)}")
            else:
                print("  Warning: No valid transfer files loaded")
                generator.transfers_df = pd.DataFrame()
        else:
            print("  No CSV files found in Transfers folder")
            generator.transfers_df = pd.DataFrame()
    else:
        print("WARNING: Transfers folder not found! Creating sample transfers...")
        generator.transfers_df = generator._create_sample_data('transfers')
    
    # Validate and process
    print("\nValidating data...")
    generator._validate_data()
    
    # Process all clients
    print("\nProcessing clients...")
    generator.process_all_clients()
    
    # Save results
    output_file = 'recommendations.csv'
    generator.save_results(output_file)
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE!")
    print(f"Results saved to: {output_file}")
    print("=" * 60)
    
    return generator.recommendations


def simple_run():
    """
    Simplified version using the main generator's auto-detection
    """
    try:
        from bcc_push_generator import BCCPushNotificationGenerator
    except ImportError:
        print("ERROR: Cannot import bcc_push_generator.py")
        print("Make sure the file exists in the current directory")
        return
    
    print("Running with automatic folder detection...")
    
    # The generator will auto-detect your folder structure
    generator = BCCPushNotificationGenerator(data_dir='.')
    
    # Override the load method to specifically look for your structure
    import pandas as pd
    from pathlib import Path
    
    # Manual loading for your specific structure
    print("\nLoading files from your folder structure...")
    
    # Load clients from root
    if Path('clients.csv').exists():
        generator.clients_df = pd.read_csv('clients.csv')
        print(f"✓ Loaded clients.csv: {len(generator.clients_df)} clients")
    
    # Load from Transactions folder
    trans_dfs = []
    if Path('Transactions').exists():
        for f in Path('Transactions').glob('*.csv'):
            trans_dfs.append(pd.read_csv(f))
        if trans_dfs:
            generator.transactions_df = pd.concat(trans_dfs, ignore_index=True)
            print(f"✓ Loaded Transactions: {len(generator.transactions_df)} records")
    
    # Load from Transfers folder
    transfer_dfs = []
    if Path('Transfers').exists():
        for f in Path('Transfers').glob('*.csv'):
            transfer_dfs.append(pd.read_csv(f))
        if transfer_dfs:
            generator.transfers_df = pd.concat(transfer_dfs, ignore_index=True)
            print(f"✓ Loaded Transfers: {len(generator.transfers_df)} records")
    
    # Process
    generator._validate_data()
    generator.process_all_clients()
    generator.save_results('recommendations.csv')
    
    print("\n✅ Complete! Check recommendations.csv")


if __name__ == "__main__":
    import sys
    
    # Check if pandas is installed
    try:
        import pandas
        import numpy
    except ImportError:
        print("ERROR: Required packages not installed!")
        print("Please run: pip install pandas numpy")
        sys.exit(1)
    
    # Run the appropriate function
    if len(sys.argv) > 1 and sys.argv[1] == '--simple':
        simple_run()
    else:
        run_with_folder_structure()