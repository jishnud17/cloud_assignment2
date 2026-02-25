from __future__ import print_function
import boto3
import json
import sys
import argparse
from decimal import Decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from pprint import pprint

# Note - Do not change the class name and constructor
# You are free to add any functions to this class without changing the specifications mentioned below.
class DynamoDBHandler:

    def __init__(self, region):
        self.client = boto3.client('dynamodb')
        self.resource = boto3.resource('dynamodb', region_name=region)

    def table_exists(self, tableName):
        """Check if a table exists in DynamoDB."""
        existing_tables = self.client.list_tables()['TableNames']
        return tableName in existing_tables

    def create_and_load_data(self, tableName, fileName):
        existing_tables = self.client.list_tables()['TableNames']

        if tableName not in existing_tables:
            try:
                table = self.resource.create_table(
                    TableName=tableName,
                    KeySchema=[
                        {"AttributeName": "year", "KeyType": "HASH"},      # Partition key
                        {"AttributeName": "title", "KeyType": "RANGE"},    # Sort key
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "year", "AttributeType": "N"},
                        {"AttributeName": "title", "AttributeType": "S"},
                    ],
                    BillingMode='PAY_PER_REQUEST',
                )
                table.wait_until_exists()
            except ClientError as err:
                print(
                    "Couldn't create table %s. Here's why: %s: %s" % (
                        tableName,
                        err.response["Error"]["Code"],
                        err.response["Error"]["Message"],
                    )
                )
                raise
        else:
            table = self.resource.Table(tableName)

        # Load data from JSON file
        with open(fileName) as f:
            movies = json.load(f, parse_float=Decimal)

        # Batch write movies, skipping entries without both actors and directors
        with table.batch_writer() as batch:
            for movie in movies:
                info = movie.get("info", {})
                # Skip entries that don't have both actors and directors
                if "actors" not in info or "directors" not in info:
                    continue
                if len(info["actors"]) == 0 or len(info["directors"]) == 0:
                    continue
                batch.put_item(Item=movie)

        return "Data loading completed"

    def insert_movie(self, tableName, title, year, directors, actors, release_date, rating):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)

        # Check if movie already exists
        try:
            response = table.get_item(
                Key={"year": year, "title": title}
            )
            if "Item" in response:
                return f"Movie {title} already exists."
        except ClientError:
            pass

        directors_list = [d.strip() for d in directors.split(",")]
        actors_list = [a.strip() for a in actors.split(",")]
        try:
            table.put_item(
                Item={
                    "year": year,
                    "title": title,
                    "info": {
                        "directors": directors_list,
                        "actors": actors_list,
                        "release_date": release_date,
                        "rating": Decimal(str(rating)),
                    },
                }
            )
        except ClientError as err:
            return "Movie %s could not be inserted - %s." % (
                title, err.response["Error"]["Message"]
            )
        return f"Movie {title} successfully inserted."

    def delete_movie(self, tableName, title):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)

        # Scan for all movies with the given title (case-insensitive)
        try:
            response = table.scan()
            all_movies = response["Items"]
            while "LastEvaluatedKey" in response:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                all_movies.extend(response["Items"])
        except ClientError as err:
            return "Movie %s could not be deleted - %s." % (
                title, err.response["Error"]["Message"]
            )

        # Find all movies matching the title (case-insensitive)
        matching_movies = [m for m in all_movies if m["title"].lower() == title.lower()]

        if len(matching_movies) == 0:
            return f"Movie {title} does not exist."

        # Delete all matching movies
        try:
            for movie in matching_movies:
                table.delete_item(
                    Key={
                        "year": movie["year"],
                        "title": movie["title"],
                    }
                )
        except ClientError as err:
            return "Movie %s could not be deleted - %s." % (
                title, err.response["Error"]["Message"]
            )
        return f"Movie {title} successfully deleted."

    def update_movie(self, tableName, title, year, directors, actors, release_date, rating):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)

        # Check if movie exists
        try:
            response = table.get_item(
                Key={"year": year, "title": title}
            )
            if "Item" not in response:
                return f"Movie {title} and {year} does not exist."
        except ClientError as err:
            return "Movie %s could not be updated - %s." % (
                title, err.response["Error"]["Message"]
            )

        # Build update expression
        update_expr_parts = []
        expr_attr_values = {}

        if directors is not None:
            directors_list = [d.strip() for d in directors.split(",")]
            update_expr_parts.append("info.directors = :d")
            expr_attr_values[":d"] = directors_list

        if actors is not None:
            actors_list = [a.strip() for a in actors.split(",")]
            update_expr_parts.append("info.actors = :a")
            expr_attr_values[":a"] = actors_list

        # Overwrite release_date and rating only when explicitly provided (including empty string)
        if release_date is not None:
            update_expr_parts.append("info.release_date = :rd")
            expr_attr_values[":rd"] = release_date

        if rating is not None:
            update_expr_parts.append("info.rating = :r")
            expr_attr_values[":r"] = Decimal(str(rating))

        update_expr = "SET " + ", ".join(update_expr_parts)

        try:
            table.update_item(
                Key={
                    "year": year,
                    "title": title,
                },
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as err:
            return "Movie %s could not be updated - %s." % (
                title, err.response["Error"]["Message"]
            )
        return f"Movie {title} successfully updated."

    def search_movie_actor(self, tableName, actor):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)

        # Scan all movies and filter case-insensitively for the actor
        try:
            response = table.scan()
            all_movies = response["Items"]
            while "LastEvaluatedKey" in response:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                all_movies.extend(response["Items"])
        except ClientError as err:
            print(
                "Couldn't scan for movies. Here's why: %s: %s" % (
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            )
            raise

        # Case-insensitive actor matching
        actor_lower = actor.lower()
        matching_movies = []
        for movie in all_movies:
            info = movie.get("info", {})
            actors_list = info.get("actors", [])
            for a in actors_list:
                if a.lower() == actor_lower:
                    movie_obj = {
                        "title": movie["title"],
                        "year": int(movie["year"]),
                        "release_date": info.get("release_date", ""),
                        "actors": actors_list,
                    }
                    matching_movies.append(movie_obj)
                    break

        if len(matching_movies) == 0:
            return f"No movies found for {actor}."

        result = {"info": matching_movies}
        pprint(result)
        return ""

    def search_movie_actor_director(self, tableName, actor, director):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)

        try:
            response = table.scan()
            all_movies = response["Items"]
            while "LastEvaluatedKey" in response:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                all_movies.extend(response["Items"])
        except ClientError as err:
            print(
                "Couldn't scan for movies. Here's why: %s: %s" % (
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            )
            raise

        # Case-insensitive actor and director matching
        actor_lower = actor.lower()
        director_lower = director.lower()
        matching_movies = []
        for movie in all_movies:
            info = movie.get("info", {})
            actors_list = info.get("actors", [])
            directors_list = info.get("directors", [])
            actor_found = any(a.lower() == actor_lower for a in actors_list)
            director_found = any(d.lower() == director_lower for d in directors_list)
            if actor_found and director_found:
                movie_obj = {
                    "title": movie["title"],
                    "year": int(movie["year"]),
                    "release_date": info.get("release_date", ""),
                    "actors": actors_list,
                }
                matching_movies.append(movie_obj)

        if len(matching_movies) == 0:
            return f"No movies found for actor {actor} and director {director}."

        result = {"info": matching_movies}
        pprint(result)
        return ""

    def print_stats(self, tableName, highest_rating, lowest_rating):
        if not self.table_exists(tableName):
            return f"Table {tableName} does not exist"

        table = self.resource.Table(tableName)
        try:
            response = table.scan()
            movies = response["Items"]
            while "LastEvaluatedKey" in response:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                movies.extend(response["Items"])
        except ClientError as err:
            print(
                "Couldn't scan table. Here's why: %s: %s" % (
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            )
            raise

        # Filter movies that have a rating
        rated_movies = [m for m in movies if "info" in m and "rating" in m.get("info", {})]

        if len(rated_movies) == 0:
            return "No rated movies found"

        if highest_rating:
            max_rating = max(m["info"]["rating"] for m in rated_movies)
            top_movies = [m for m in rated_movies if m["info"]["rating"] == max_rating]
            result_list = []
            for movie in top_movies:
                info = movie.get("info", {})
                result_list.append({
                    "title": movie["title"],
                    "year": int(movie["year"]),
                    "release_date": info.get("release_date", ""),
                    "actors": info.get("actors", []),
                    "directors": info.get("directors", []),
                    "rating": float(info.get("rating", 0)),
                })
            pprint({"info": result_list})
            return ""
        elif lowest_rating:
            min_rating = min(m["info"]["rating"] for m in rated_movies)
            bottom_movies = [m for m in rated_movies if m["info"]["rating"] == min_rating]
            result_list = []
            for movie in bottom_movies:
                info = movie.get("info", {})
                result_list.append({
                    "title": movie["title"],
                    "year": int(movie["year"]),
                    "release_date": info.get("release_date", ""),
                    "actors": info.get("actors", []),
                    "directors": info.get("directors", []),
                    "rating": float(info.get("rating", 0)),
                })
            pprint({"info": result_list})
            return ""
        else:
            return "Please specify --highest_rating_movies or --lowest_rating_movies"

    def delete_table(self, tableName):
        if not self.table_exists(tableName):
            return f"Table {tableName} do not exist."

        table = self.resource.Table(tableName)
        try:
            table.delete()
            table.wait_until_not_exists()
        except ClientError as err:
            print(
                "Couldn't delete table %s. Here's why: %s: %s" % (
                    tableName,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            )
            raise
        return f"Table {tableName} successfully deleted."

    def check_valid_insert_movie_args(self, args):
        # Year, Title, Actors, Directors cannot be empty
        if args.title is None or args.year is None or args.directors is None or args.actors is None or args.release_date is None or args.rating is None:
            return False
        # Year must be 4 digits
        if args.year < 1000 or args.year > 9999:
            return False
        if len(args.title.strip()) == 0:
            return False
        if len(args.directors.strip()) == 0:
            return False
        if len(args.actors.strip()) == 0:
            return False
        if len(args.release_date.strip()) == 0:
            return False
        return True

    def check_valid_update_movie_args(self, args):
        # Year, Title, Actors, Directors cannot be empty
        if args.title is None or args.year is None or args.directors is None or args.actors is None:
            return False
        if args.year < 1000 or args.year > 9999:
            return False
        if len(args.title.strip()) == 0:
            return False
        if len(args.directors.strip()) == 0:
            return False
        if len(args.actors.strip()) == 0:
            return False
        return True

    def dispatch(self, args):
        action = args.action
        response = ''

        # For all actions except create_and_load_data, check if table exists
        if action not in ('create_and_load_data', 'delete_table'):
            if not self.table_exists(args.table_name):
                return f"Table {args.table_name} does not exist"

        if action == 'create_and_load_data':
            if args.table_name is None or args.file_name is None:
                response = 'Please provide the table name and file name'
            else:
                response = self.create_and_load_data(args.table_name, args.file_name)
        elif action == 'insert_movie':
            if not self.check_valid_insert_movie_args(args):
                if args.year is not None and (args.year < 1000 or args.year > 9999):
                    response = 'year should be 4 digit'
                else:
                    response = ('Please provide the table name, title, year, directors, ' +
                        'actors, release_date and rating\nExample usage: python ' +
                        'dynamodb_handler.py insert_movie --title "The Big New Movie" ' +
                        '--year 2015 --directors "Larry" --actors "Moe" ' +
                        '--release_date "23 Jan 2018" --rating 5.5')
            else:
                response = self.insert_movie(args.table_name, args.title, args.year, args.directors, args.actors, args.release_date, args.rating)
        elif action == 'delete_movie':
            if args.title is None:
                response = 'Please provide the title of the movie to delete'
            else:
                response = self.delete_movie(args.table_name, args.title)
        elif action == 'update_movie':
            if not self.check_valid_update_movie_args(args):
                if args.year is not None and (args.year < 1000 or args.year > 9999):
                    response = 'year should be 4 digit'
                else:
                    response = 'Please provide the title, year, directors, and actors of the movie to update'
            else:
                response = self.update_movie(args.table_name, args.title, args.year, args.directors, args.actors, args.release_date, args.rating)
        elif action == 'search_movie_actor':
            if args.actor is None:
                response = 'Please provide the actor name to search'
            else:
                response = self.search_movie_actor(args.table_name, args.actor)
        elif action == 'search_movie_actor_director':
            if args.actor is None or args.director is None:
                response = 'Please provide both the actor and director names to search'
            else:
                response = self.search_movie_actor_director(args.table_name, args.actor, args.director)
        elif action == 'print_stats':
            response = self.print_stats(args.table_name, args.highest_rating_movies, args.lowest_rating_movies)
        elif action == 'delete_table':
            response = self.delete_table(args.table_name)

        return response


def main():
    parser = argparse.ArgumentParser(description='dynamic_handler')
    operations = ['create_and_load_data',
               'insert_movie',
               'delete_movie',
               'update_movie',
               'search_movie_actor',
               'search_movie_actor_director',
               'print_stats',
               'delete_table',
               ]

    parser.add_argument('action', help="command", choices=operations)

    # no need to specify the table_name, always use the default table name
    parser.add_argument("--table_name", type=str, help="name of the table", default="Movies")
    parser.add_argument("--file_name", type=str, help="name of the file")

    parser.add_argument("-y", "--year", type=int, help="year of the movie")
    parser.add_argument("-t", "--title", type=str, help="title of the movie")
    # directors could be single director or multiple directors separated by comma
    # this directors is used in insert_movie and update_movie
    parser.add_argument("--directors", type=str, help="director(s) of the movie")
    # actors could be single actor or multiple actors separated by comma
    # this actors is used in insert_movie and update_movie
    parser.add_argument("--actors", type=str, help="actors(s) in the movie")
    parser.add_argument("--release_date", type=str, help="release date of the movie (23 Jan 2018)")
    parser.add_argument("--rating", type=float, help="rating of the movie")

    # this actor is used in search_movie_actor and search_movie_actor_director
    parser.add_argument("--actor", type=str, help="actor in the movie")
    # this director is used in search_movie_actor_director
    parser.add_argument("--director", type=str, help="director of the movie")

    # we assume the user does not set both highest_rating_movies and lowest_rating_movies
    # optional flag for highest_rating_movies
    parser.add_argument("--highest_rating_movies", action="store_true", help="flag to get highest rating movies")
    # optional flag for lowest_rating_movies
    parser.add_argument("--lowest_rating_movies", action="store_true", help="flag to get lowest rating movies")

    # optional flag for setting the region (no need to specify the region, always use us-west-2 as default region)
    parser.add_argument('--region', type=str, help='The region name', default='us-west-2')

    args = parser.parse_args()
    handler = DynamoDBHandler(args.region)
    response = handler.dispatch(args)
    print(response)

    # example usage
    # python dynamodb_handler.py create_and_load_data --file_name moviedata.json
    # python dynamodb_handler.py insert_movie --year 2015 --title "The Big New Movie" --directors "Evan Goldberg, Seth Rogen" --actors "James Franco, Jonah Hill" --release_date "23 Jan 2018" --rating 5.5
    # python dynamodb_handler.py delete_movie --title "The Big New Movie"
    # python dynamodb_handler.py update_movie --year 2015 --title "The Big New Movie" --directors "Evan Goldberg, Seth Rogen" --actors "James Franco, Jonah Hill" --release_date "23 Jan 2018" --rating 6.5
    # python dynamodb_handler.py search_movie_actor --actor "Moe"
    # python dynamodb_handler.py search_movie_actor_director --actor "Moe" --director "Evan Goldberg"
    # python dynamodb_handler.py print_stats --highest_rating_movies
    # python dynamodb_handler.py print_stats --lowest_rating_movies
    # python dynamodb_handler.py delete_table

if __name__ == '__main__':
    main()