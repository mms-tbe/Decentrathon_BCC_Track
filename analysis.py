import pandas as pd

def analyze_recommendations():
    """
    Analyzes the target recommendations to find correlations in the data.
    """
    # Load the data
    clients = pd.read_csv('organized_data/merged/clients_merged.csv')
    transactions = pd.read_csv('organized_data/merged/transactions_merged.csv')
    transfers = pd.read_csv('organized_data/merged/transfers_merged.csv')
    target_recommendations = pd.read_csv('target_recommendations.csv')

    # Merge clients and target recommendations
    data = pd.merge(clients, target_recommendations, left_on='name', right_on='client_name')

    # Calculate total transaction and transfer amounts for each client
    trans_agg = transactions.groupby('client_code')['amount'].agg(['sum', 'mean', 'count']).rename(columns={'sum': 'total_trans_amount', 'mean': 'avg_trans_amount', 'count': 'trans_count'})
    transf_agg = transfers.groupby('client_code')['amount'].agg(['sum', 'mean', 'count']).rename(columns={'sum': 'total_transf_amount', 'mean': 'avg_transf_amount', 'count': 'transf_count'})

    # Merge with main data
    data = pd.merge(data, trans_agg, on='client_code', how='left')
    data = pd.merge(data, transf_agg, on='client_code', how='left')

    # Analyze the characteristics for each recommended product
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    for product in data['recommended_product'].unique():
        print(f"--- Analysis for: {product} ---")
        product_data = data[data['recommended_product'] == product]
        print(f"Number of clients: {len(product_data)}")
        print("Client Statuses:")
        print(product_data['status'].value_counts())
        print("\nAverage values:")
        print(product_data[['age', 'avg_monthly_balance_KZT', 'total_trans_amount', 'avg_trans_amount', 'trans_count', 'total_transf_amount', 'avg_transf_amount', 'transf_count']].mean())
        print("\n")

if __name__ == "__main__":
    analyze_recommendations()
