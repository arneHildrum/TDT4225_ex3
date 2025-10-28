import os
import ast
import sys
import pandas as pd
from pprint import pprint 
from pymongo import MongoClient, version
from collections import defaultdict
from itertools import combinations


def run(program):
    task = 100
    while(task != 0):
        task = int(input("Enter task number (0-10): "))
        if task == 0:
            print("Shutting down program.......")
        else:
            match task:
                case 1: program.task_1()
                case 2: program.task_2()
                case 3: program.task_3()
                case 4: program.task_4()
                case 5: program.task_5()
                case 6: program.task_6()
                case 7: program.task_7()
                case 8: program.task_8()
                case 9: program.task_9()
                case 10: program.task_10()

def main():
    program = None
    try:
        program = Program()
        program.setup()
        program.show_coll()
        run(program)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


class DbConnector:
    """
    """

    def __init__(self,
                 DATABASE="movies",
                 HOST="localhost",
                 PORT=27017,
                 USER="root",
                 PASSWORD="secret123"):
        uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:{PORT}/?authSource=admin"
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)




class Program:

    
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    
    def setup(self, csv_folder = "."):
        db = self.client['movies']
        for filename in os.listdir(csv_folder):
            if filename.endswith(".csv"):
                collection_name = filename.replace(".csv", "")
                file_path = os.path.join(csv_folder, filename)

                try:
                    df = pd.read_csv(file_path)
                    df = df.where(pd.notnull(df), None)
                    for col in df.columns:
                        if df[col].astype(str).str.startswith("[").any():
                            try:
                                df[col] = df[col].apply(
                                    lambda x: ast.literal_eval(x)
                                    if isinstance(x, str) and x.strip().startswith("[")
                                    else x
                                )
                            except Exception as e:
                                print(f"Skipping parsing of column {col} due to error: {e}")
                    data = df.to_dict("records")
                    if data:
                            db[collection_name].insert_many(data)
                except Exception as e:
                    print(f"ERROR: Failed to process {filename}: ", e)

    
    def show_coll(self):
        collections = self.client['movies'].list_collection_names()
        print(collections)


    def task_1(self):
        pipeline = [
        {'$unwind': '$crew'},

        {'$match': {'crew.job': 'Director'}},

        {'$lookup': {
            'from': 'movies',
            'localField': 'id',
            'foreignField': 'id',
            'as': 'movie'
        }},

        {'$unwind': '$movie'},

        {'$group': {
            '_id': {
                'director_id': '$crew.id',
                'director_name': '$crew.name'
            },
            'movie_count': {'$sum': 1},
            'revenues': {'$push': '$movie.revenue'},
            'vote_averages': {'$avg': '$movie.vote_average'}
        }},

        {'$match': {'movie_count': {'$gte': 5}}},

        {'$addFields': {
            'sorted_revenues': {'$sortArray': {'input': '$revenues', 'sortBy': 1}},
        }},

        {'$addFields': {
            'median_revenue': {
                '$let': {
                    'vars': {
                        'size': {'$size': '$sorted_revenues'},
                        'mid': {'$floor': {'$divide': [{'$size': '$sorted_revenues'}, 2]}}
                    },
                    'in': {
                        '$cond': [
                            {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                            {'$avg': [
                                {'$arrayElemAt': ['$sorted_revenues', '$$mid']},
                                {'$arrayElemAt': ['$sorted_revenues', {'$subtract': ['$$mid', 1]}]}
                            ]},
                            {'$arrayElemAt': ['$sorted_revenues', '$$mid']}
                        ]
                    }
                }
            }
        }},

        {'$sort': {'median_revenue': -1}},

        {'$limit': 10},

        
        {'$project': {
            '_id': 0,
            'director_name': '$_id.director_name',
            'director_id': '$_id.director_id',
            'movie_count': 1,
            'median_revenue': {'$round': ['$median_revenue', 2]},
            'mean_vote_average': {'$round': ['$vote_averages', 2]}
        }}
    ]


        results = list(self.db.movies.aggregate(pipeline))
        if results:
            print(f"\n{'Rank':<5} {'Director':<30} {'Movies':<8} {'Median Revenue':<18} {'Mean Vote Avg':<15}")
            print("-"*80)
            for i, result in enumerate(results, 1):
                print(f"{i:<5} {result['director_name']:<30} {result['movie_count']:<8} "
                    f"${result['median_revenue']:>15,.2f} {result['mean_vote_average']:>15.2f}")


    def task_2(self):
        pipeline = [
            {'$lookup': {
                'from': 'movies',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'movie'
            }},
            {'$unwind': '$movie'},
            {'$project': {
                'movie_id': '$id',
                'vote_average': '$movie.vote_average',
                'cast': {
                    '$map': {
                        'input': '$cast',
                        'as': 'actor',
                        'in': {
                            'id': '$$actor.id',
                            'name': '$$actor.name'
                        }
                    }
                }
            }}
        ]

        movies_with_cast = list(self.db.credits.aggregate(pipeline))

        pair_data = defaultdict(lambda: {'count': 0, 'votes': []})

        for movie in movies_with_cast:
            cast = movie.get('cast', [])
            vote_avg = movie.get('vote_average', 0)

            for actor1, actor2 in combinations(cast, 2):
                if actor1['id'] < actor2['id']:
                    pair_key = (actor1['id'], actor1['name'], actor2['id'], actor2['name'])
                else:
                    pair_key = (actor2['id'], actor2['name'], actor1['id'], actor1['name'])

                pair_data[pair_key]['count'] += 1
                pair_data[pair_key]['votes'].append(vote_avg)

        results = []
        for (actor1_id, actor1_name, actor2_id, actor2_name), data in pair_data.items():
            if data['count'] >= 3:
                avg_vote = sum(data['votes']) / len(data['votes']) if data['votes'] else 0
                results.append({
                    'actor1': actor1_name,
                    'actor2': actor2_name,
                    'co_appearances': data['count'],
                    'avg_vote_average': round(avg_vote, 2)
                })

        results.sort(key=lambda x: (-x['co_appearances'], -x['avg_vote_average']))
        if results:
            print(f"\n{'Rank':<5} {'Actor 1':<30} {'Actor 2':<30} {'Co-Appearances':<15} {'Avg Vote':<10}")
            print("-"*100)
            for i, result in enumerate(results[:50], 1):
                print(f"{i:<5} {result['actor1']:<30} {result['actor2']:<30} "
                    f"{result['co_appearances']:<15} {result['avg_vote_average']:<10.2f}")
            



    def task_3(self):
        pipeline = [
            {'$unwind': '$cast'},

            {'$lookup': {
                'from': 'movies',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'movie'
            }},

            {'$unwind': '$movie'},

            {'$unwind': '$movie.genres_list'},

            {'$group': {
                '_id': {
                    'actor_id': '$cast.id',
                    'actor_name': '$cast.name'
                },
                'movie_count': {'$sum': 1},
                'genres': {'$addToSet': '$movie.genres_list'}
            }},

            {'$match': {'movie_count': {'$gte': 10}}},

            {'$addFields': {
                'genre_count': {'$size': '$genres'},
                'example_genres': {'$slice': ['$genres', 5]}
            }},

            {'$sort': {'genre_count': -1, 'movie_count': -1}},

            {'$limit': 10},

            
            {'$project': {
                '_id': 0,
                'actor': '$_id.actor_name',
                'movie_count': 1,
                'genre_count': 1,
                'example_genres': 1
            }}
        ]

        results = list(self.db.credits.aggregate(pipeline))

        if results:
            print(f"\n{'Rank':<5} {'Actor':<30} {'Movies':<8} {'Genres':<8} {'Example Genres':<60}")
            print("-"*120)
            for i, result in enumerate(results, 1):
                genres_str = ', '.join(result['example_genres'])
                print(f"{i:<5} {result['actor']:<30} {result['movie_count']:<8} "
                    f"{result['genre_count']:<8} {genres_str:<60}")


    def task_4(self):
        pipeline = [
            {'$match': {
                'belongs_to_collection': {'$ne': None},
                'belongs_to_collection.name': {'$exists': True}
            }},

            {'$group': {
                '_id': '$belongs_to_collection.name',
                'movie_count': {'$sum': 1},
                'total_revenue': {'$sum': '$revenue'},
                'vote_averages': {'$push': '$vote_average'},
                'release_dates': {'$push': '$release_date'}
            }},

            {'$match': {'movie_count': {'$gte': 3}}},

            {'$addFields': {
                'sorted_votes': {'$sortArray': {'input': '$vote_averages', 'sortBy': 1}},
                'sorted_dates': {'$sortArray': {'input': '$release_dates', 'sortBy': 1}}
            }},

            {'$addFields': {
                'median_vote_average': {
                    '$let': {
                        'vars': {
                            'size': {'$size': '$sorted_votes'},
                            'mid': {'$floor': {'$divide': [{'$size': '$sorted_votes'}, 2]}}
                        },
                        'in': {
                            '$cond': [
                                {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                                {'$avg': [
                                    {'$arrayElemAt': ['$sorted_votes', '$$mid']},
                                    {'$arrayElemAt': ['$sorted_votes', {'$subtract': ['$$mid', 1]}]}
                                ]},
                                {'$arrayElemAt': ['$sorted_votes', '$$mid']}
                            ]
                        }
                    }
                },
                'earliest_date': {'$arrayElemAt': ['$sorted_dates', 0]},
                'latest_date': {'$arrayElemAt': ['$sorted_dates', -1]}
            }},

            {'$sort': {'total_revenue': -1}},

            {'$limit': 10},

            
            {'$project': {
                '_id': 0,
                'collection': '$_id',
                'movie_count': 1,
                'total_revenue': 1,
                'median_vote_average': {'$round': ['$median_vote_average', 2]},
                'date_range': {
                    '$concat': [
                        {'$ifNull': ['$earliest_date', 'N/A']},
                        ' → ',
                        {'$ifNull': ['$latest_date', 'N/A']}
                    ]
                }
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        if results:
            print(f"\n{'Rank':<5} {'Collection':<35} {'Movies':<8} {'Total Revenue':<18} {'Med. Vote':<11} {'Date Range':<30}")
            print("-"*120)
            for i, result in enumerate(results, 1):
                print(f"{i:<5} {result['collection']:<35} {result['movie_count']:<8} "
                    f"${result['total_revenue']:>15,.0f} {result['median_vote_average']:>11.2f} {result['date_range']:<30}")


    def task_5(self):
        pipeline = [
            {'$match': {
                'release_date': {'$ne': None, '$exists': True},
                'runtime': {'$ne': None, '$gt': 0},
                'genres_list': {'$exists': True, '$ne': []}
            }},
            {'$addFields': {
                'year': {
                    '$toInt': {
                        '$substr': ['$release_date', 0, 4]
                    }
                },
                'primary_genre': {'$arrayElemAt': ['$genres_list', 0]}
            }},

            {'$addFields': {
                'decade': {
                    '$concat': [
                        {'$toString': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]}},
                        's'
                    ]
                }
            }},

            {'$group': {
                '_id': {
                    'decade': '$decade',
                    'primary_genre': '$primary_genre'
                },
                'movie_count': {'$sum': 1},
                'runtimes': {'$push': '$runtime'}
            }},

            {'$addFields': {
                'sorted_runtimes': {'$sortArray': {'input': '$runtimes', 'sortBy': 1}}
            }},

            {'$addFields': {
                'median_runtime': {
                    '$let': {
                        'vars': {
                            'size': {'$size': '$sorted_runtimes'},
                            'mid': {'$floor': {'$divide': [{'$size': '$sorted_runtimes'}, 2]}}
                        },
                        'in': {
                            '$cond': [
                                {'$eq': [{'$mod': ['$$size', 2]}, 0]},
                                {'$avg': [
                                    {'$arrayElemAt': ['$sorted_runtimes', '$$mid']},
                                    {'$arrayElemAt': ['$sorted_runtimes', {'$subtract': ['$$mid', 1]}]}
                                ]},
                                {'$arrayElemAt': ['$sorted_runtimes', '$$mid']}
                            ]
                        }
                    }
                }
            }},

            {'$sort': {'_id.decade': 1, 'median_runtime': -1}},

            
            {'$project': {
                '_id': 0,
                'decade': '$_id.decade',
                'primary_genre': '$_id.primary_genre',
                'movie_count': 1,
                'median_runtime': {'$round': ['$median_runtime', 1]}
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        if results:
            current_decade = None
            for result in results:
                if current_decade != result['decade']:
                    if current_decade is not None:
                        print()
                    current_decade = result['decade']
                    print(f"\n{current_decade}")
                    print("-"*80)
                    print(f"{'Primary Genre':<25} {'Movies':<10} {'Median Runtime (min)':<20}")
                    print("-"*80)

                print(f"{result['primary_genre']:<25} {result['movie_count']:<10} {result['median_runtime']:<20.1f}")
    

    def task_6(self):
        pipeline = [
            {'$lookup': {
                'from': 'movies',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'movie'
            }},

            {'$unwind': '$movie'},

            {'$match': {
                'movie.release_date': {'$ne': None, '$exists': True}
            }},

            {'$unwind': '$cast'},

            {'$match': {
                'cast.order': {'$lte': 4},
                'cast.gender': {'$in': [1, 2]}
            }},

            {'$addFields': {
                'year': {
                    '$toInt': {
                        '$substr': ['$movie.release_date', 0, 4]
                    }
                }
            }},

            {'$addFields': {
                'decade': {
                    '$concat': [
                        {'$toString': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]}},
                        's'
                    ]
                }
            }},

            {'$group': {
                '_id': {
                    'movie_id': '$id',
                    'decade': '$decade'
                },
                'female_count': {
                    '$sum': {
                        '$cond': [{'$eq': ['$cast.gender', 1]}, 1, 0]
                    }
                },
                'total_count': {'$sum': 1}
            }},

            {'$addFields': {
                'female_proportion': {
                    '$divide': ['$female_count', '$total_count']
                }
            }},

            {'$group': {
                '_id': '$_id.decade',
                'movie_count': {'$sum': 1},
                'avg_female_proportion': {'$avg': '$female_proportion'}
            }},

            {'$sort': {'_id': 1}},

            
            {'$project': {
                '_id': 0,
                'decade': '$_id',
                'movie_count': 1,
                'avg_female_proportion': {'$round': ['$avg_female_proportion', 4]}
            }}
        ]

        results = list(self.db.credits.aggregate(pipeline))

        if results:
            print(f"\n{'Decade':<15} {'Movie Count':<15} {'Avg Female Proportion':<25}")
            print("-"*80)
            for result in results:
                percentage = result['avg_female_proportion'] * 100
                print(f"{result['decade']:<15} {result['movie_count']:<15} "
                    f"{result['avg_female_proportion']:<10.4f} ({percentage:.2f}%)")


    def task_7(self):
        pipeline = [
            {'$match': {
                '$or': [
                    {'overview': {'$regex': 'noir', '$options': 'i'}},
                    {'tagline': {'$regex': 'noir', '$options': 'i'}}
                ],
                'vote_count': {'$gte': 50}
            }},

            {'$addFields': {
                'year': {
                    '$cond': {
                        'if': {'$and': [
                            {'$ne': ['$release_date', None]},
                            {'$gte': [{'$strLenCP': '$release_date'}, 4]}
                        ]},
                        'then': {'$substr': ['$release_date', 0, 4]},
                        'else': 'N/A'
                    }
                }
            }},

            {'$sort': {'vote_average': -1}},

            {'$limit': 20},

            
            {'$project': {
                '_id': 0,
                'title': 1,
                'year': 1,
                'vote_average': {'$round': ['$vote_average', 2]},
                'vote_count': 1,
                'overview': 1
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        if results:
            print(f"\n{'Rank':<5} {'Title':<50} {'Year':<6} {'Vote Avg':<10} {'Vote Count':<12}")
            print("-"*100)
            for i, result in enumerate(results, 1):
                print(f"{i:<5} {result['title'][:48]:<50} {result['year']:<6} "
                    f"{result['vote_average']:<10.2f} {result['vote_count']:<12}")


    def task_8(self):
        pipeline = [
            {'$lookup': {
                'from': 'movies',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'movie'
            }},

            {'$unwind': '$movie'},

            {'$match': {
                'movie.vote_count': {'$gte': 100}
            }},

            {'$unwind': '$crew'},
            {'$match': {'crew.job': 'Director'}},

            {'$addFields': {
                'director': '$crew'
            }},

            {'$unwind': '$cast'},

            {'$group': {
                '_id': {
                    'director_id': '$director.id',
                    'director_name': '$director.name',
                    'actor_id': '$cast.id',
                    'actor_name': '$cast.name'
                },
                'collaboration_count': {'$sum': 1},
                'avg_vote_average': {'$avg': '$movie.vote_average'},
                'avg_revenue': {'$avg': '$movie.revenue'}
            }},

            {'$match': {'collaboration_count': {'$gte': 3}}},

            {'$sort': {'avg_vote_average': -1}},

            {'$limit': 20},

            
            {'$project': {
                '_id': 0,
                'director': '$_id.director_name',
                'actor': '$_id.actor_name',
                'films_count': '$collaboration_count',
                'mean_vote_average': {'$round': ['$avg_vote_average', 2]},
                'mean_revenue': {'$round': ['$avg_revenue', 2]}
            }}
        ]

        results = list(self.db.credits.aggregate(pipeline))

        if results:
            print(f"\n{'Rank':<5} {'Director':<25} {'Actor':<25} {'Films':<7} {'Mean Vote':<11} {'Mean Revenue':<18}")
            print("-"*120)
            for i, result in enumerate(results, 1):
                print(f"{i:<5} {result['director'][:23]:<25} {result['actor'][:23]:<25} "
                    f"{result['films_count']:<7} {result['mean_vote_average']:<11.2f} "
                    f"${result['mean_revenue']:>15,.2f}")


    def task_9(self):
        pipeline = [
            {'$match': {
                'original_language': {'$ne': 'en', '$exists': True},
                '$or': [
                    {'production_companies.name': {'$regex': 'United States', '$options': 'i'}},
                    {'production_countries.name': 'United States of America'}
                ]
            }},

            {'$group': {
                '_id': '$original_language',
                'count': {'$sum': 1},
                'example_title': {'$first': '$title'}
            }},

            {'$sort': {'count': -1}},

            {'$limit': 10},

            {'$project': {
                '_id': 0,
                'language': '$_id',
                'count': 1,
                'example_title': 1
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        if results:
            print(f"\n{'Rank':<5} {'Language':<15} {'Count':<10} {'Example Title':<60}")
            print("-"*100)
            for i, result in enumerate(results, 1):
                print(f"{i:<5} {result['language']:<15} {result['count']:<10} {result['example_title'][:58]:<60}")


    def task_10(self):
        pipeline = [
            {'$lookup': {
                'from': 'movies',
                'localField': 'movieId',
                'foreignField': 'id',
                'as': 'movie'
            }},

            {'$unwind': '$movie'},

            {'$unwind': {
                'path': '$movie.genres_list',
                'preserveNullAndEmptyArrays': True
            }},

            {'$group': {
                '_id': '$userId',
                'ratings_count': {'$sum': 1},
                'ratings': {'$push': '$rating'},
                'genres': {'$addToSet': '$movie.genres_list'}
            }},

            {'$match': {'ratings_count': {'$gte': 20}}},

            {'$addFields': {
                'mean_rating': {'$avg': '$ratings'},
                'genre_count': {'$size': '$genres'}
            }},

            {'$addFields': {
                'variance': {
                    '$divide': [
                        {
                            '$reduce': {
                                'input': '$ratings',
                                'initialValue': 0,
                                'in': {
                                    '$add': [
                                        '$$value',
                                        {'$pow': [
                                            {'$subtract': ['$$this', '$mean_rating']},
                                            2
                                        ]}
                                    ]
                                }
                            }
                        },
                        '$ratings_count'
                    ]
                }
            }},
            {'$project': {
                '_id': 0,
                'userId': '$_id',
                'ratings_count': 1,
                'variance': {'$round': ['$variance', 4]},
                'genre_count': 1
            }}
        ]

        all_users = list(self.db.ratings.aggregate(pipeline))

        genre_diverse = sorted(all_users, key=lambda x: x['genre_count'], reverse=True)[:10]

        high_variance = sorted(all_users, key=lambda x: x['variance'], reverse=True)[:10]

        if genre_diverse:
            print(f"\n{'Rank':<5} {'User ID':<15} {'Ratings Count':<15} {'Genre Count':<15} {'Variance':<15}")
            print("-"*100)
            for i, user in enumerate(genre_diverse, 1):
                print(f"{i:<5} {user['userId']:<15} {user['ratings_count']:<15} "
                    f"{user['genre_count']:<15} {user['variance']:<15.4f}")

        if high_variance:
            print(f"\n{'Rank':<5} {'User ID':<15} {'Ratings Count':<15} {'Variance':<15} {'Genre Count':<15}")
            print("-"*100)
            for i, user in enumerate(high_variance, 1):
                print(f"{i:<5} {user['userId']:<15} {user['ratings_count']:<15} "
                    f"{user['variance']:<15.4f} {user['genre_count']:<15}")


if __name__ == '__main__':
    main()
