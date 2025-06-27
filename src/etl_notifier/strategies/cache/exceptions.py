class CacheError(Exception):
    """Base exception for cache-related errors"""

    pass


class CacheLoadError(CacheError):
    """Exception raised when loading from cache fails"""

    pass


class CacheSaveError(CacheError):
    """Exception raised when saving to cache fails"""

    pass
