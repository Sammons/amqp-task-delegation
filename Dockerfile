FROM node

RUN npm install --global mocha;

WORKDIR /test

CMD ["mocha", "test"]