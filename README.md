Для того щоб кожен раз при білді не використовувати датасети юзайте:\
**bash:** `docker build -t bigdataproject . && docker run -v $(pwd):/app bigdataproject`\
**cmd:** `docker build -t bigdataproject . && docker run -v ${pwd}:/app bigdataproject`