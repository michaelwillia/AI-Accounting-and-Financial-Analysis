"""CLI entrypoint for AI bookkeeping MVP."""

from db import init_db
from service import handle_message


def main() -> None:
    """Run the command-line loop for user interaction."""
    init_db()
    print("AI对话记账系统已启动，输入 exit 退出。")

    while True:
        user_input = input("你：").strip()
        if user_input.lower() == "exit":
            print("系统：已退出。")
            break
        if not user_input:
            print("系统：请输入内容。")
            continue

        result = handle_message(user_input)
        print(f"系统：{result}")


if __name__ == "__main__":
    """Program startup function call."""
    main()
