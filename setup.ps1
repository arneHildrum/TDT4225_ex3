docker run -d mongodb/mongodb-enterprise-server:latest --name mongodb -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=secret123
docker ps
docker exec -it mongodb mongosh -u root -p secret123 --authenticationDatabase admin