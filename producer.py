from shuttle import Shuttle


if __name__ == '__main__':
    bootstrap_servers = ["ec2-xxxx-xxx-xx.ap-northeast-2.compute.amazonaws.com:9092"]
    shuttle = Shuttle(
        bootstrap_servers= bootstrap_servers
    )
    shuttle.init_producer()
    res = {
        'res':1
    }
    shuttle.send(topic='test02', value=res)