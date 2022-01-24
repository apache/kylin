import os

CUR_DIR = os.path.dirname(__file__)
BACKUP_PATH = os.path.join(CUR_DIR, '..', 'backup')
JARS_PATH = os.path.join(BACKUP_PATH, 'jars')
SCRIPTS_PATH = os.path.join(BACKUP_PATH, 'scripts')
TARS_PATH = os.path.join(BACKUP_PATH, 'tars')
KYLIN_PROPERTIES_DIR = os.path.join(BACKUP_PATH, 'properties')

KYLIN_PROPERTIES_TEMPLATE_DIR = os.path.join(KYLIN_PROPERTIES_DIR, '{cluster_num}')
PROPERTIES_TEMPLATE_DIR = os.path.join(KYLIN_PROPERTIES_DIR, 'templates')

TEMPLATE_OF_KYLIN_PROPERTIES = 'kylin.properties.template'
RENDERED_FILE = '.rendered'

ROOT_DIR = os.path.join(CUR_DIR, '..')
TEMPLATES_PATH = os.path.join(ROOT_DIR, 'cloudformation_templates')
