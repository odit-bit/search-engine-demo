## Go demo application
### search engine that can input or search link and ranking the page in GO 



In the nutshell user will submit link (url) from `frontend` and it will insert into `graph`, and backend service will do pipeline process to `crawler` the link, extract the text and title content for `index` and ranked by `pagerank`.

for live demo http://103.13.207.175:8080

Project structure:
```
root
├── frontend
│   └── ...
├── crawler
│   └── ...
├── graph 
│   └── ...
├── pagerank 
│   └── ...
├── index 
│   └── ...
├── ...

```

`frontend` is user facing interface (html page) for communicated with backend ,with pre-rendering html page (template) for every endpoint.

`crawler` service background that process link from graph. for more https://github.com/odit-bit/webcrawler

`graph` API manage of link and edge with grpc. for more https://github.com/odit-bit/linkstore

`pagerank` calculate ranking for indexing

`index` API manage index link-extracted-text-content as doc. for more https://github.com/odit-bit/indexstore


build with docker compose.
```
docker-compose up -d --build
```
compose 2.
```
docker compose up -d --build
```

those code will create an image from *.dockerfile and run the containers service.

try visit localhost:8080/