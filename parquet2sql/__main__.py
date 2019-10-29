from parquet2sql.parquet2sql import get_parser, parquet2sql


def main():
    parser = get_parser()
    args = parser.parse_args()
    parquet2sql(parquet=args.parquet, table=args.table, db=args.db)
    return 0


if __name__ == "__main__":
    exit(main())
