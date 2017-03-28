FROM node:7.7-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app
RUN npm install && npm cache clean

CMD ["node", "server.js"]
