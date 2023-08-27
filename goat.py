import pandas as pd

def positions_gained(input_csv, output_csv):
    
    data = pd.read_csv(input_csv)

    # Convert 'grid' and 'position' columns to numeric values
    data['grid'] = pd.to_numeric(data['grid'], errors='coerce')
    data['position'] = pd.to_numeric(data['position'], errors='coerce')

    # Calculate positions gained for each driver
    data['positions_gained'] = data['grid'] - data['position']
    positions_gained_by_driver = data.groupby('driverId')['positions_gained'].sum()

    # DataFrame for the results
    results_df = pd.DataFrame({
        'driverId': positions_gained_by_driver.index,
        'TotalPositionsGained': positions_gained_by_driver.values
    })

    # Save as CSV
    results_df.to_csv(output_csv, index=False)  

def goatfinder(positions_csv, drivers_csv):

    # Read Data
    positions_data = pd.read_csv(positions_csv)
    drivers_data = pd.read_csv(drivers_csv)

    # Merge positions gained and drivers data using driverId
    merged_data = pd.merge(positions_data, drivers_data, on='driverId')

    # Save merged data to goat.csv
    merged_data['forename'] = merged_data['forename'].str.upper()
    merged_data['surname'] = merged_data['surname'].str.upper()
    merged_data.to_csv('goat.csv', index=False)

    # Find the driver with the most wins
    most_wins_driver = merged_data.loc[merged_data['TotalPositionsGained'].idxmax()]

    # Generate text file
    with open('goat.txt', 'w') as txt_file:
        txt_file.write(f"    ___  \n   [-+-]   \n []=/|\\=[]  \n   /:|:\\ \n  | /U\\ | \n  || _ ||   {most_wins_driver['forename']} {most_wins_driver['surname']} IS THE GOAT!!!\n  '\\(@)/' \n    \\Y/  \n []==U==[] \n  ___H___   \n  `--V--`⠀   ")

# Main code
positions_gained('results.csv', 'positions_gained.csv')
goatfinder('positions_gained.csv', 'drivers.csv')

