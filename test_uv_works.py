from polars_utils import utils


def main(num: int):
    print(f"Your number is {num}")


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--num",
            "A number",
        ),
    )
    main(
        num=args.num,
    )
