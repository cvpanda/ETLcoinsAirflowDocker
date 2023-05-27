from api.apicalls import CoinApi


def main():
    api = CoinApi()
    print("Iniciando..")
    try:
        api.insert_db()
        print("Se inserto correctamente")
    except Exception as e:
        print(f"Algo paso: {str(e)}")
    finally:
        api.close_connection()


if __name__ == "__main__":
    main()
