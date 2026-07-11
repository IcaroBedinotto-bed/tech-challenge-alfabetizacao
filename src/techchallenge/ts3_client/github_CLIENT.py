import requests


class GithubClient:

    @staticmethod
    def download_file(url, destination):
        response = requests.get(url)
        response.raise_for_status()

        with open(destination, "wb") as f:
            f.write(response.content)