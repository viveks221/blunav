FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --production --silent || npm ci --production --silent

COPY . .

EXPOSE 3000
CMD ["node", "src/index.js"]
