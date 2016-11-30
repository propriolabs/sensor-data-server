var config = {
    es_host: 'localhost:9200',
    es_index: 'proprio',
    es_index_rankings: 'rankings',
    es_type: 'raw_v2',
    es_analysis_type: 'analysis_v2',
    es_score_type: 'scores',
    es_users_type: 'users',
    es_session_type: 'sessions',
    es_audio_labels_type:'audio_labels',
    server_port: 8080,
    analysis_url: 'http://localhost:5000/tennis',
    server_key: 'server.key',
    server_cert:'server.crt'
};

module.exports = config;
