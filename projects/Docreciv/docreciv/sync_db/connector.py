"""
Управление подключениями к MongoDB для sync_db.

Предоставляет connection pooling и управление клиентами.
"""
from typing import Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from ..core.config import get_config
from ..core.exceptions import ConnectionError


class MongoConnector:
    """
    Менеджер подключений к MongoDB с connection pooling.
    """
    
    def __init__(self):
        self.config = get_config().sync_db
        self._remote_client: Optional[MongoClient] = None
        self._local_client: Optional[MongoClient] = None
    
    def get_remote_client(self) -> MongoClient:
        """
        Получает клиент MongoDB для удаленной базы данных.
        
        Returns:
            MongoClient для удаленной MongoDB
        
        Raises:
            ConnectionError: При ошибке подключения
        """
        if self._remote_client is None:
            try:
                remote_config = self.config.remote_mongo
                
                if not remote_config.password:
                    raise ConnectionError(
                        "MONGO_PASSWORD not set for remote MongoDB",
                        context={"mongo_server": remote_config.server}
                    )
                
                url = remote_config.get_connection_url()
                
                client = MongoClient(
                    url,
                    tls=True,
                    tlsCAFile=remote_config.ssl_cert,
                    tlsAllowInvalidHostnames=True,
                    serverSelectionTimeoutMS=20000,
                    maxPoolSize=10
                )
                
                # Проверка подключения
                client.admin.command("ping")
                self._remote_client = client
                
            except ConnectionFailure as e:
                raise ConnectionError(
                    f"Failed to connect to remote MongoDB: {e}",
                    context={"mongo_server": self.config.remote_mongo.server},
                    original_error=e
                )
            except Exception as e:
                raise ConnectionError(
                    f"Unexpected error connecting to remote MongoDB: {e}",
                    context={"mongo_server": self.config.remote_mongo.server},
                    original_error=e
                )
        
        return self._remote_client
    
    def get_local_client(self) -> MongoClient:
        """
        Получает клиент MongoDB для локальной базы данных.
        
        Returns:
            MongoClient для локальной MongoDB
        
        Raises:
            ConnectionError: При ошибке подключения
        """
        if self._local_client is None:
            try:
                local_config = self.config.local_mongo
                
                if not local_config.password:
                    raise ConnectionError(
                        "MONGO_METADATA_PASSWORD not set for local MongoDB",
                        context={"mongo_server": local_config.server}
                    )
                
                url = local_config.get_connection_url()
                
                client = MongoClient(
                    url,
                    serverSelectionTimeoutMS=10000,
                    maxPoolSize=10
                )
                
                # Проверка подключения
                client.admin.command("ping")
                self._local_client = client
                
            except ConnectionFailure as e:
                raise ConnectionError(
                    f"Failed to connect to local MongoDB: {e}",
                    context={"mongo_server": self.config.local_mongo.server},
                    original_error=e
                )
            except Exception as e:
                raise ConnectionError(
                    f"Unexpected error connecting to local MongoDB: {e}",
                    context={"mongo_server": self.config.local_mongo.server},
                    original_error=e
                )
        
        return self._local_client
    
    def close(self) -> None:
        """Закрывает все подключения."""
        if self._remote_client:
            self._remote_client.close()
            self._remote_client = None
        
        if self._local_client:
            self._local_client.close()
            self._local_client = None

