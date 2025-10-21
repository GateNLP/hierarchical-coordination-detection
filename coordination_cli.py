import argparse

from service.coordination.data_sources.csv_handler import read_post
from service.coordination.processing.coordination import calcCoordination


def main(input_csv: str, exclude_file: str, complexity: int, output_csv: str):

    # load the posts taking into account anything we should exclude
    posts, raw_data = read_post(input_csv, exclude_file)

    # determine the coordination
    result = calcCoordination(posts, complexity)

    # Write out the final results
    result.to_csv(output_csv, encoding="utf-8", index=False)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(prog="Hierarchical Coordination Detection")
    
    parser.add_argument("input_csv",
        help="CSV file of posts to process. See documentation for column names etc.")
    
    parser.add_argument("exclude_file",
        help="List of linking entities to exclude; file must exist but can be empty")
    
    parser.add_argument("output_csv",
        help="CSV file to write results into (will be overwritten if it already exists")
    
    parser.add_argument("-c", "--complexity", type=int, choices=[1,2,3], default=3,
        help="Algorithm complexity 1) shared the same links, 2) pairwise level coordination, 3) pairwise and group level coordination.")
    
    args = parser.parse_args()
    
    main(args.input_csv, args.exclude_file, args.complexity, args.output_csv)
