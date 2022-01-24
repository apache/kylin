import logging

from instances.kylin_instance import KylinInstance

logger = logging.getLogger(__name__)


class KylinUtils:

    @staticmethod
    def is_kylin_accessible(kylin_address: str, port: str = '7070') -> bool:
        kylin = KylinInstance(host=kylin_address, port=port)
        try:
            assert kylin.client.await_kylin_start(
                check_action=kylin.client.check_login_state,
                timeout=1800,
                check_times=3
            )
        except AssertionError:
            logger.error('Check kylin status failed, please check the public ip whether is opened.')
            return False
        return True
