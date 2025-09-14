import pandas as pd

def compare_results():
    """
    Compares the generated recommendations with the target recommendations.
    """
    my_results = pd.read_csv('recommendations.csv')
    target_results = pd.read_csv('target_recommendations.csv')
    clients = pd.read_csv('clients.csv')

    # Merge to align by client_code
    my_results = pd.merge(my_results, clients[['client_code', 'name']], on='client_code')

    comparison = pd.merge(my_results, target_results, left_on='name', right_on='client_name', suffixes=('_my', '_target'))

    matches = comparison['product'] == comparison['recommended_product']
    match_rate = matches.sum() / len(comparison)

    print(f"Match rate: {match_rate:.2%}")
    print("\nMismatches:")
    print(comparison[~matches][['name', 'product', 'recommended_product']])

if __name__ == "__main__":
    compare_results()
