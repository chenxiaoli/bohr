import settings

MONGO_HOST = settings.DB_HOST
MONGO_PORT = settings.DB_PORT
MONGO_USERNAME = settings.DB_USER
MONGO_PASSWORD = settings.DB_PASSWORD
MONGO_AUTH_SOURCE = "admin"
MONGO_DBNAME = settings.DB_NAME

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S%Z'
URL_PREFIX = "api"
API_VERSION = "v1"
X_DOMAINS = '*'
X_HEADERS = ["authorization", ]

QUERY_MAX_RESULTS = "pageSize"

trade_node = {
    "item_title": "交易平台",
    'datasource': {
        'source': 'trade_node',
    },
    'additional_lookup': {
        'url': 'regex("[\w]+")',
        'field': 'code'
    },
    'schema': {
        'code': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 45,
        }, 'name': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 45,
        }, 'url': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 200,
        }, 'icon': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 200,
        }, "symbols": {
            "type": "list",
            "items": [
                {
                    'schema': {
                        'code': {
                            'type': 'string',
                            'minlength': 1,
                            'maxlength': 45,
                        }, 'base': {
                            'type': 'string',
                            'minlength': 1,
                            'maxlength': 45,
                        }, 'quote': {
                            'type': 'string',
                            'minlength': 1,
                            'maxlength': 45,
                        }, 'pricePrecision': {
                            'type': 'int'
                        }, 'amountPrecision': {
                            'type': 'int'
                        },
                        'symbolPartition': {
                            'type': 'string',
                            'minlength': 1,
                            'maxlength': 45,
                        },
                        'dataEnable': {
                            'type': 'bool',
                        },
                    }
                }
            ]
        }
    }
}

coin = {
    'schema': {
        'symbol': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 100,
        },
        'domain': {
            'type': 'string',
            'minlength': 1,
            'maxlength': 100,
        },
        'type': {
            'type': 'string',
        },
        "name": {
            'type': 'string',
        },
        "description": {
            'type': 'string',
        },
        "icon": {
            'type': 'string',
        }
    }
}

DOMAIN = {
    "data/trade/node": trade_node,
    "data/trade/coin": coin
}
