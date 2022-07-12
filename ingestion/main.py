import logging
import logging.config
import yaml
from ingestion.adapter.file_system import FileSystemConnector
from ingestion.adapter.pg import PostgresConnector
from ingestion.application.mk_extractor import SourceConfig, TargetConfig, MKExtractor


def main():
    """
        entry point to run EL Job
    """
    config_path = '/Users/brayanjules/etl_practice1/ingestion_tool/ingestion/config/ingest_config.yml'
    # Get Input Arg
    #parser = argparse.ArgumentParser(description='Run the Ingestion EL job.')
    #parser.add_argument('config', help='A configuration file in YAML format.')
    #args = parser.parse_args()
    # Parsing YAML file
    config = yaml.safe_load(open(config_path)) #args.config

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading source configuration
    source_config = SourceConfig(**config['source'])
    # reading target configuration
    target_config = TargetConfig(**config['target'])

    pg_conn_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(target_config.user,
                                                               target_config.password,
                                                               target_config.host,
                                                               target_config.port,
                                                               target_config.db)
    pg_connector = PostgresConnector(conn_string=pg_conn_string)

    fs_connector = FileSystemConnector(root_path=source_config.root_path)
    mk_extractor = MKExtractor(pg_connector, fs_connector, source_config, target_config)
    logger.info('EL job started')
    mk_extractor.extract_load()
    logger.info('EL job finished')


if __name__ == '__main__':
    main()
