import pandas as pd
import os
import io

input_file_path = 'batch_analysis_summary.csv'
output_directory = 'processed_output'
output_filename = 'total_flux_batch_analysis_summary.csv'

flux_columns = ['Total Red Flux', 'Total Yellow Flux', 'Total Green Flux', 'Total Blue Flux']

group_id_dict = {
    'pl1d3pa2': 'Plate 1 (INFg+) Passage 2 Day 3',
    'pl1d4pa2': 'Plate 1 (INFg+) Passage 2 Day 4',
    'pl2d4pa1': 'Plate 2 (INFg-) Passage 1 Day 4'
}

def extract_well_info(sample_name):
    parts = sample_name.split('_') 
    group_id = '_'.join(parts[:-1]) # 'pl1d3pa2_A01.fcs' -> 'pl1d3pa2'
    
    well_id = parts[-1].split('.')[0] # 'A01.fcs' -> 'A01'
    plate_row = well_id[0] # 'A01' -> 'A'
    
    try:
        plate_column = str(int(well_id[1:])) # '01' -> '1'
    except ValueError:
        plate_column = well_id[1:]  # '01' -> '01'
    
    return group_id, plate_row, plate_column

def create_transposed_flux_summary(input_file_path: str, output_directory: str, output_filename: str):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    output_file_path = os.path.join(output_directory, output_filename)

    try:
        df = pd.read_csv(input_file_path)

        extracted_data = df['Sample_Name'].apply(lambda x: extract_well_info(x)).tolist()

        df[['Group_ID', 'Plate_Row', 'Plate_Column']] = extracted_data
        
        cols_to_keep = ['Group_ID', 'Plate_Row', 'Plate_Column'] + flux_columns
        df_agg = df[cols_to_keep].groupby(['Group_ID', 'Plate_Row', 'Plate_Column'], as_index=False).mean()
        
        group_ids = df_agg['Group_ID'].unique()

        with open(output_file_path, 'w', newline='') as f:

            master_header_row = ['']
            group_col_counts = {}

            for group_id in group_ids:
                cols = df_agg[df_agg['Group_ID'] == group_id]['Plate_Column'].unique()
                group_col_counts[group_id] = len(cols)

            for i, group_id in enumerate(group_ids):
                group_header = group_id_dict.get(group_id, group_id)
                num_cols = group_col_counts[group_id]
                
                if i < len(group_ids) - 1:
                    num_cols += 1
                
                master_header_row.extend([group_header] + [''] * (num_cols - 1))
                
            f.write(','.join(master_header_row) + '\n')
            f.write('\n') # blank line 1 below master header
            
            for flux_type in flux_columns:
                
                flux_header_row = ['']
                dfs_to_concat = []
                
                for i, group_id in enumerate(group_ids):
                    df_group = df_agg[df_agg['Group_ID'] == group_id]
                    
                    pivot_df = df_group.pivot(
                        index='Plate_Row',
                        columns='Plate_Column',
                        values=flux_type
                    ).sort_index()
                    
                    all_columns = df_group['Plate_Column'].unique()
                    sorted_cols = [str(col) for col in sorted(int(c) for c in all_columns)]
                    pivot_df = pivot_df.reindex(columns=sorted_cols)

                    pivot_df.index.name = ''
                    dfs_to_concat.append(pivot_df)

                    if i < len(group_ids) - 1:
                        separator_df = pd.DataFrame(
                            {f'sep_col_{i}': [''] * len(pivot_df)}, 
                            index=pivot_df.index
                        )
                        dfs_to_concat.append(separator_df) 

                        flux_header_row.extend([flux_type] + [''] * len(sorted_cols))                      
                    else:
                        flux_header_row.extend([flux_type] + [''] * (len(sorted_cols) - 1))

                f.write(','.join(flux_header_row) + '\n')
                f.write('\n') # blank line 2 below flux header
                
                df_combined = pd.concat(dfs_to_concat, axis=1)

                output_buffer = io.StringIO()
                # scientific notation
                df_combined.to_csv(output_buffer, header=True, index=True, float_format='%.2E')
                output_buffer.seek(0)
                
                header_line = output_buffer.readline().strip()

                modified_header_line = header_line

                for i in range(len(group_ids) - 1):
                    sep_col_name = f',sep_col_{i}'
                    modified_header_line = modified_header_line.replace(sep_col_name, ',')
                
                f.write(modified_header_line + '\n')
                
                for line in output_buffer:
                    f.write(line)
                
                f.write('\n\n') # two blank lines for separation

        print(f"saved the file at {output_file_path}")

    except FileNotFoundError:
        print(f"file not found at {input_file_path}")
    except Exception as e:
        print(f"{e}")

create_transposed_flux_summary(
    input_file_path=input_file_path, 
    output_directory=output_directory,
    output_filename=output_filename
)