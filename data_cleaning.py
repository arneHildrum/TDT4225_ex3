import pandas as pd
import ast 


def clean_data_cr(data):
    """Clean the credits dataset."""
    rows = len(data)

    data = data.dropna(subset=['id'])                                                                                                               # Drop rows missing id
    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')                                                                         # Convert id to int 
    data = data[~(
        (data['cast'].isna() | (data['cast'].astype(str).str.strip() == "[]")) &
        (data['crew'].isna() | (data['crew'].astype(str).str.strip() == "[]"))
    )]                                                                                                                                              # Drop rows missing both cast and crew

#    data['cast'] = data['cast'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])                                                       # Parse JSON to Python lists
#    data['crew'] = data['crew'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])

    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')                                                                         # convert id column to int

    print("Percentage of data remaining after cleaning = ", round(len(data) / rows * 100, 2), "%")
    return data


def clean_data_kw(data):
    """Clean the keywords dataset."""
    rows = len(data)

    data = data[~((data['id'].isna() | (data['keywords'].astype(str).str.strip() == "[]")))]                                                        # Drop rows missing id or keyword
    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')                                                                         # Convert id to int 
#    data['keywords'] = data['keywords'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])                                               # Parse JSON to Python lists

    print("Percentage of data remaining after cleaning = ", round(len(data) / rows * 100, 2), "%")
    return data


def clean_data_ln(data):
    """Clean the links  dataset."""
    rows = len(data)

    data = data.dropna(subset=['movieId'])                                                                                                          # Drop rows missing id
    data['movieId'] = pd.to_numeric(data['movieId'], errors='coerce').astype('Int64')
    data['imdbId'] = pd.to_numeric(data['imdbId'], errors='coerce').astype('Int64')
    data['tmdbId'] = pd.to_numeric(data['tmdbId'], errors='coerce').astype('Int64')

    data = data.dropna(subset=['imdbId', 'tmdbId'], how='all')                                                                                      # Drop rows with missing ids

    print("Percentage of data remaining after cleaning = ", round(len(data) / rows * 100, 2), "%")
    return data


def clean_data_md(data):
    """Clean the movies_metadata dataset."""
    rows = len(data)

    data = data.dropna(subset=['id'])                                                                                                               # Drop rows missing id
    data['id'] = pd.to_numeric(data['id'], errors='coerce').astype('Int64')                                                                         # Convert id to int 
    data['id'] = pd.to_numeric(data['vote_count'], errors='coerce').astype('Int64')                                                                 # Convert vote count to int 

    data = data.drop(columns=['adult', 'video'], errors='ignore')                                                                                   # Drop adult and video columns because they do not add data dimensionality

    numeric_cols = ['budget', 'revenue', 'runtime', 'vote_average', 'vote_count', 'popularity']                                                     # Ensure consistent data types
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors='coerce')

        json_cols = [
        'belongs_to_collection', 'genres', 'production_companies',
        'production_countries', 'spoken_languages'
    ]
#    for col in json_cols:
#        data[col] = data[col].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) and x.strip().startswith("[") else [])    #  Parse dictionary-like columns

    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')                                                                    # Convert release_date to datetime        



    print("Percentage of data remaining after cleaning = ", round(len(data) / rows * 100, 2), "%")
    return data


def clean_data_rt(data):
    """Clean the ratings  dataset."""
    rows = len(data)

    data = data.dropna(subset=['movieId', 'rating'], how='any')                                                                                      # Drop rows missing movie or rating

    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s', errors='coerce')                                                                 # Convert timestamp to datetime

    data['rating_raw'] = pd.to_numeric(data['rating'], errors='coerce')

    mapped = (data['rating_raw'] * 2).round()
    valid_mask = data['rating_raw'].between(0.5, 5.0)
    mapped = mapped.where(valid_mask, other=pd.NA)                                                                                                   # Convert float rating to int
    data['rating'] = mapped.astype('Int64')
    data = data.drop(columns=['rating_raw'])


    print("Percentage of data remaining after cleaning = ", round(len(data) / rows * 100, 2), "%")
    return data


if __name__ == "__main__":
    
    dp = pd.read_csv("credits.csv")
    dp = clean_data_cr(dp)
    dp.to_csv("credits.csv", index=False)

    dp = pd.read_csv("keywords.csv")
    dp = clean_data_kw(dp)
    dp.to_csv("keywords.csv", index=False)

    dp = pd.read_csv("links.csv")
    dp = clean_data_ln(dp)
    dp.to_csv("links.csv", index=False)

    dp = pd.read_csv("movies_metadata.csv", low_memory=False)
    dp = clean_data_md(dp)
    dp.to_csv("movies_metadata.csv", index=False)

    dp = pd.read_csv("ratings.csv")
    dp = clean_data_rt(dp)
    dp.to_csv("ratings.csv", index=False)
