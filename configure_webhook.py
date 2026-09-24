from getpass import getpass
from retry_controller import SECRET_PATH, save_webhook_secret


def main() -> None:
    print("请输入轮换后的企业微信群机器人 Webhook。输入内容不会显示，也不会写入代码仓库。")
    webhook = getpass("Webhook URL: ").strip()
    save_webhook_secret(webhook)
    print(f"已使用当前 Windows 用户的 DPAPI 加密保存到：{SECRET_PATH}")


if __name__ == "__main__":
    main()
