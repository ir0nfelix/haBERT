# Habr NER Scraper: Архитектура и спецификация API

Данный проект предназначен для высокоскоростного сбора статей и новостей с платформы Habr.com с целью их последующей векторизации и извлечения именованных сущностей (NER - Named Entity Recognition). Проект базируется на непубличном API Хабра, что позволяет извлекать чистые данные без необходимости рендеринга страниц через headless-браузеры.

## 1. Анатомия контента: Три сущности Хабра

Архитектура платформы строится вокруг единого сквозного счетчика идентификаторов (`id`). Все публикации получают уникальный инкрементный идентификатор, но при этом жестко разделяются бэкендом на три разные сущности:

* **Статьи** (`postType = "article"`): Полноценные лонгриды, туториалы и аналитика.
* **Новости** (`postType = "news"`): Короткие информационные сводки.
* **Посты** (`postType = "post"`): Самостоятельный формат микроблоггинга (ИТ-заметки), не имеющий отдельной внутренней страницы.

> **Важное ограничение API:** Хабр использует единый эндпоинт `/kek/v2/articles/{id}/` для получения полных данных. Однако этот эндпоинт отдает только Статьи и Новости. При попытке запросить по этому адресу карточку Поста, бэкенд возвращает ошибку `404 Not Found` с кодом `POST_TYPE_MISMATCH` и сообщением «Не совпадает тип публикации».

## 2. Стратегия сбора: Парсинг перебором ID (Brute-force)

Учитывая сквозную нумерацию `id` и доступность Статей и Новостей через один эндпоинт, применяется стратегия последовательного перебора идентификаторов.

### Поведение парсера при переборе:

* **Успех (200 OK):** Если под текущим `id` скрывается статья или новость, API возвращает полный JSON с метаданными и полным HTML-кодом в корневом поле `textHtml`. Эти данные сохраняются в базу.
* **Ошибка (404 POST_TYPE_MISMATCH):** Если под текущим `id` скрывается пост, ошибка игнорируется, и происходит переход к следующему `id`. Это позволяет отфильтровать ненужный формат микроблогов прямо на этапе HTTP-запроса.
* **Ошибка (404 Not Found):** Если публикация удалена, скрыта в черновики или заблокирована модерацией, этот `id` пропускается.

## 3. Сводная таблица полей API (Статьи vs Новости)

Анализ ответов API показывает, что структура карточки Статьи и карточки Новости абсолютно идентична. Бэкенд возвращает унифицированный формат данных.

| Поле в JSON (от корня) | Описание данных | Пример: Статья JSON | Пример: Новость JSON |
| :--- | :--- | :--- | :--- |
| `id` | Сквозной идентификатор публикации | `"1041202"` | `"1068052"` |
| `postType` | Внутренний тип сущности | `"article"` | `"news"` |
| `timePublished` | Дата и время в формате ISO | `"2026-08-07T11:59:40+00:00"` | `"2026-08-07T16:07:37+00:00"` |
| `isCorporative` | Опубликовано ли в блоге компании | `true` | `false` |
| `titleHtml` | Заголовок (может содержать спецсимволы) | `"Семь примеров самого странного применения баз данных в мире"` | `"Claude Opus 5 удалила данные пользователя вместо создания резервной копии..."` |
| `leadData.textHtml` | HTML-код анонса (до ката) | Присутствует | Присутствует |
| `textHtml` | Полный HTML-код публикации | Присутствует | Присутствует |
| `author.alias` | Никнейм автора | `"T1_IT"` | `"denis-19"` |
| `author.isSeo` | Маркер SEO-аккаунта | `false` | `false` |
| `author.scoreStats.score` | Карма / рейтинг автора | `17` | `1066` |
| `author.scoreStats.votesCount` | Количество голосов за карму автора | `41` | `2000` |
| `statistics.score` | Итоговый рейтинг публикации | `0` | `1` |
| `statistics.votesCount` | Всего голосов за публикацию | `0` | `5` |
| `statistics.votesCountPlus` | Количество плюсов | `0` | `2` |
| `statistics.votesCountMinus` | Количество минусов | `0` | `3` |
| `statistics.readingCount` | Количество прочтений | `172` | `571` |
| `statistics.readers` | Количество уникальных читателей | `147` | `513` |
| `statistics.reach` | Охват публикации | `922` | `1426` |
| `statistics.favoritesCount` | Добавлений в закладки | `4` | `1` |
| `statistics.commentsCount` | Количество комментариев | `0` | `9` |
| `hubs[].alias` | Алиас хаба | `"db_admins"` | `"infosecurity"` |
| `hubs[].title` | Название хаба (русский классификатор) | `"Базы данных"` | `"Информационная безопасность"` |
| `hubs[].isProfiled` | Является ли хаб профильным | `true` | `true` |
| `flows[].alias` | Алиас глобального потока | `"admin", "popsci"` | `"develop", "admin", "management", "popsci"` |
| `tags[].titleHtml` | Пользовательские теги | `"базы данных", "способы применения"` | `"Claude Opus 5", "rm -rf"` |

## 4. Примеры ответов API

### Пример карточки Новости (ID 1068052)

**Запрос:** `GET https://habr.com/kek/v2/articles/1068052/`

```json
{
  "id": "1068052",
  "timePublished": "2026-08-07T16:07:37+00:00",
  "isCorporative": false,
  "lang": "ru",
  "titleHtml": "Claude Opus 5 удалила данные пользователя вместо создания резервной копии с помощью «rm -rf» для всего диска на ПК",
  "leadData": {
    "textHtml": "<p>ИИ‑агент Anthropic Claude Opus 5&nbsp;во&nbsp;время выполнения запроса пользователя «сделать резервное копирование на&nbsp;ПК» <a href=\"https://www.reddit.com/r/ClaudeCode/comments/1vg18yu/claude_rm_rf_ed_my_pc/\" rel=\"noopener noreferrer nofollow\">перепутал</a> правильное написание каталога и написал «/c/Users/harih» вместо каталога «C:\\Users\\harih». После этого процедура резервное копирования пошла не&nbsp;по&nbsp;плану. ИИ решил стереть полученную папку (резервную копию в&nbsp;неправильной директории), но&nbsp;применил команду rm ‑rf для&nbsp;всего диска на&nbsp;ПК.</p>",
    "imageUrl": "https://habrastorage.org/getpro/habr/upload_files/b9d/a5e/685/b9da5e685a264ca1abb25c6884d65bcc.webp",
    "buttonTextHtml": "Читать далее",
    "image": {
      "url": "https://habrastorage.org/getpro/habr/upload_files/b9d/a5e/685/b9da5e685a264ca1abb25c6884d65bcc.webp",
      "fit": "cover",
      "positionY": 0,
      "positionX": 0
    }
  },
  "editorVersion": "2.0",
  "postType": "news",
  "postLabels": [],
  "author": {
    "id": "780987",
    "alias": "denis-19",
    "fullname": "Денис",
    "avatarUrl": "//habrastorage.org/getpro/habr/avatars/6a0/f31/235/6a0f31235bb26134f3c7a741c03e4efb.jpg",
    "speciality": "Информационная служба Хабра, без использования ИИ",
    "deleted": false,
    "isSeo": false,
    "scoreStats": {
      "score": 1066,
      "votesCount": 2000
    },
    "rating": 633.7,
    "relatedData": {
      "vote": {
        "value": null
      },
      "canVote": false,
      "votePlus": {
        "canVote": false,
        "isKarmaEnough": false,
        "isChargeEnough": false,
        "isPublicationLimitEnough": true
      },
      "voteMinus": {
        "canVote": false,
        "isKarmaEnough": false,
        "isChargeEnough": false,
        "isPublicationLimitEnough": false
      },
      "isSubscribed": false
    },
    "contacts": [
      {
        "title": "Telegram",
        "url": "https://telegram.me/habr_com_news",
        "value": "habr_com_news",
        "siteTitle": null,
        "favicon": null
      }
    ],
    "authorContacts": [
      {
        "title": "Telegram",
        "url": "https://telegram.me/habr_com_news",
        "value": "habr_com_news",
        "siteTitle": null,
        "favicon": null
      }
    ],
    "paymentDetails": {
      "paymentYandexMoney": null,
      "paymentPayPalMe": null,
      "paymentWebmoney": null
    },
    "donationsMethod": null,
    "isInBlacklist": false,
    "careerProfile": null,
    "isShowScores": true,
    "reach": "512K+"
  },
  "statistics": {
    "commentsCount": 9,
    "favoritesCount": 1,
    "readingCount": 571,
    "score": 1,
    "votesCount": 5,
    "votesCountPlus": 2,
    "votesCountMinus": 3,
    "reach": 1426,
    "readers": 513
  },
  "hubs": [
    {
      "id": "21922",
      "alias": "artificial_intelligence",
      "type": "collective",
      "title": "Искусственный интеллект",
      "titleHtml": "Искусственный интеллект",
      "isProfiled": false,
      "relatedData": {
        "isSubscribed": false
      }
    },
    {
      "id": "50",
      "alias": "infosecurity",
      "type": "collective",
      "title": "Информационная безопасность",
      "titleHtml": "Информационная безопасность",
      "isProfiled": true,
      "relatedData": {
        "isSubscribed": false
      }
    },
    {
      "id": "19439",
      "alias": "machine_learning",
      "type": "collective",
      "title": "Машинное обучение",
      "titleHtml": "Машинное обучение",
      "isProfiled": true,
      "relatedData": {
        "isSubscribed": false
      }
    },
    {
      "id": "221",
      "alias": "sys_admin",
      "type": "collective",
      "title": "Системное администрирование",
      "titleHtml": "Системное администрирование",
      "isProfiled": true,
      "relatedData": {
        "isSubscribed": false
      }
    },
    {
      "id": "20682",
      "alias": "pm",
      "type": "collective",
      "title": "Управление проектами",
      "titleHtml": "Управление проектами",
      "isProfiled": true,
      "relatedData": {
        "isSubscribed": false
      }
    }
  ],
  "flows": [
    {
      "id": "1",
      "alias": "develop",
      "title": "Разработка",
      "titleHtml": "Разработка"
    },
    {
      "id": "6",
      "alias": "admin",
      "title": "Администрирование",
      "titleHtml": "Администрирование"
    },
    {
      "id": "3",
      "alias": "management",
      "title": "Менеджмент",
      "titleHtml": "Менеджмент"
    },
    {
      "id": "7",
      "alias": "popsci",
      "title": "Научпоп",
      "titleHtml": "Научпоп"
    }
  ],
  "relatedData": {
    "vote": {
      "value": null,
      "voteTimeExpired": "2026-09-06T16:07:37+00:00"
    },
    "unreadCommentsCount": 9,
    "bookmarked": false,
    "canComment": true,
    "canEdit": false,
    "canViewVotes": false,
    "votePlus": {
      "canVote": false,
      "isChargeEnough": false,
      "isKarmaEnough": false,
      "isVotingOver": false,
      "isPublicationLimitEnough": true
    },
    "voteMinus": {
      "canVote": false,
      "isChargeEnough": false,
      "isKarmaEnough": false,
      "isVotingOver": false,
      "isPublicationLimitEnough": false
    },
    "draftReason": null,
    "lockReason": null,
    "canModerateComments": false,
    "trackerSubscribed": false,
    "emailSubscribed": false
  },
  "textHtml": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><figure class=\"full-width \"><img src=\"https://habrastorage.org/r/w1560/getpro/habr/upload_files/b9d/a5e/685/b9da5e685a264ca1abb25c6884d65bcc.webp\" width=\"1080\" height=\"1440\" sizes=\"(max-width: 780px) 100vw, 50vw\" srcset=\"https://habrastorage.org/r/w780/getpro/habr/upload_files/b9d/a5e/685/b9da5e685a264ca1abb25c6884d65bcc.webp 780w,&#10;       https://habrastorage.org/r/w1560/getpro/habr/upload_files/b9d/a5e/685/b9da5e685a264ca1abb25c6884d65bcc.webp 781w\" loading=\"lazy\" decode=\"async\"/></figure><p>ИИ‑агент Anthropic Claude Opus 5 во время выполнения запроса пользователя «сделать резервное копирование на ПК» <a href=\"https://www.reddit.com/r/ClaudeCode/comments/1vg18yu/claude_rm_rf_ed_my_pc/\" rel=\"noopener noreferrer nofollow\">перепутал</a> правильное написание каталога и написал «/c/Users/harih» вместо каталога «C:\\Users\\harih». После этого процедура резервное копирования пошла не по плану. ИИ решил стереть полученную папку (резервную копию в неправильной директории), но применил команду rm ‑rf для всего диска на ПК.</p><p>«Я попросил Claude Opus 5 создать резервную копию. Вместо этого он создал резервную копию в неправильном каталоге, а затем выполнил команду rm -rf для всего моего диска. Удалив всё, он просто ответил: „Извините, опечатка“, как будто ничего не случилось. Это одновременно самый смешной и самый неприятный момент с ИИ, который у меня был», — пояснил пользователь.</p><p>Ранее ИИ‑модель Claude Opus 5 Max от Anthropic <a href=\"https://www.reddit.com/r/Anthropic/comments/1v9iurd/and_just_like_that_opus_5_ultracode_wipes_the/#lightbox\" rel=\"noopener noreferrer nofollow\">удалила</a> базу данных проекта другого разработчика. По его словам, запрос, который привёл к удалению, <a href=\"https://habr.com/ru/news/1064918/\" rel=\"noopener noreferrer nofollow\">предложила</a> сама Opus 5 Max после анализа репозитория проекта на GitHub. После удаления модель признала ошибку. Разработчик смог восстановить 96 страниц проекта из 117 с помощью Gemini 3.6.</p><figure class=\"full-width \"><img src=\"https://habrastorage.org/r/w1560/getpro/habr/upload_files/da9/4c9/594/da94c95941fab6f03695a42b42b4f19b.webp\" width=\"1080\" height=\"483\" sizes=\"(max-width: 780px) 100vw, 50vw\" srcset=\"https://habrastorage.org/r/w780/getpro/habr/upload_files/da9/4c9/594/da94c95941fab6f03695a42b42b4f19b.webp 780w,&#10;       https://habrastorage.org/r/w1560/getpro/habr/upload_files/da9/4c9/594/da94c95941fab6f03695a42b42b4f19b.webp 781w\" loading=\"lazy\" decode=\"async\"/></figure><p>В июле в вайб‑кодерском стартапе BridgeMind <a href=\"https://habr.com/ru/news/1058744/\" rel=\"noopener noreferrer nofollow\">рассказали</a>, как нейросеть OpenAI GPT-5.6 Sol лишила компанию всех пользователей и подписчиков (с активной подпиской Stripe) за несколько секунд, запустив код с пустым полем во время обработки заявок. </p><p>Также похожей историей поделился ИИ‑инвестор и предприниматель Мэтт Шумер. По его словам, нейросеть OpenAI GPT-5.6 Sol в режиме Ultra с полным доступом к системе случайно стёрла все файлы на его рабочем Mac, а потом ИИ признался в ошибке. «GPT-5.6-Sol только что случайно удалил почти ВСЕ файлы на моём Mac. И именно поэтому я доверяю Fable в 1000 раз больше», — <a href=\"https://habr.com/ru/news/1058140/\" rel=\"noopener noreferrer nofollow\">рассказал </a>Шумер.</p><figure class=\"full-width \"><img src=\"https://habrastorage.org/r/w1560/getpro/habr/upload_files/d72/115/d7f/d72115d7f29381d7d64c783cce59659b.jpg\" width=\"1276\" height=\"1280\" sizes=\"(max-width: 780px) 100vw, 50vw\" srcset=\"https://habrastorage.org/r/w780/getpro/habr/upload_files/d72/115/d7f/d72115d7f29381d7d64c783cce59659b.jpg 780w,&#10;       https://habrastorage.org/r/w1560/getpro/habr/upload_files/d72/115/d7f/d72115d7f29381d7d64c783cce59659b.jpg 781w\" loading=\"lazy\" decode=\"async\"/></figure></div>",
  "tags": [
    {
      "titleHtml": "Claude Opus 5"
    },
    {
      "titleHtml": "rm -rf"
    }
  ],
  "metadata": {
    "stylesUrls": [],
    "scriptUrls": [],
    "shareImageUrl": "https://habr.com/share/publication/1068052/4e0b88ef35fd7601ef2dc7189c26f415/",
    "shareImageWidth": 1200,
    "shareImageHeight": 630,
    "vkShareImageUrl": "https://habr.com/share/publication/1068052/4e0b88ef35fd7601ef2dc7189c26f415/?format=vk",
    "schemaJsonLd": "{\"@context\":\"http:\\/\\/schema.org\",\"@type\":\"Article\",\"mainEntityOfPage\":{\"@type\":\"WebPage\",\"@id\":\"https:\\/\\/habr.com\\/ru\\/news\\/1068052\\/\"},\"headline\":\"Claude Opus 5 удалила данные пользователя вместо создания резервной копии с помощью «rm -rf» для всего диска на ПК\",\"datePublished\":\"2026-08-07T19:07:37+03:00\",\"dateModified\":\"2026-08-07T19:51:57+03:00\",\"author\":{\"@type\":\"Person\",\"name\":null},\"publisher\":{\"@type\":\"Organization\",\"name\":\"Habr\",\"logo\":{\"@type\":\"ImageObject\",\"url\":\"https:\\/\\/habrastorage.org\\/webt\\/a_\\/lk\\/9m\\/a_lk9mjkccjox-zccjrpfolmkmq.png\"}},\"description\":\"ИИ‑агент Anthropic Claude Opus 5&nbsp;во&nbsp;время выполнения запроса пользователя «сделать резервное копирование на&nbsp;ПК» перепутал правильное написание...\",\"url\":\"https:\\/\\/habr.com\\/ru\\/news\\/1068052\\/#post-content-body\",\"about\":[\"h_artificial_intelligence\",\"h_infosecurity\",\"h_machine_learning\",\"h_sys_admin\",\"h_pm\",\"f_develop\",\"f_admin\",\"f_management\",\"f_popsci\"],\"image\":[\"https:\\/\\/habrastorage.org\\/getpro\\/habr\\/upload_files\\/b9d\\/a5e\\/685\\/b9da5e685a264ca1abb25c6884d65bcc.webp\",\"https:\\/\\/habrastorage.org\\/r\\/w1560\\/getpro\\/habr\\/upload_files\\/da9\\/4c9\\/594\\/da94c95941fab6f03695a42b42b4f19b.webp\",\"https:\\/\\/habrastorage.org\\/r\\/w1560\\/getpro\\/habr\\/upload_files\\/d72\\/115\\/d7f\\/d72115d7f29381d7d64c783cce59659b.jpg\"]}",
    "metaDescription": "ИИ‑агент Anthropic Claude Opus 5&nbsp;во&nbsp;время выполнения запроса пользователя «сделать резервное копирование на&nbsp;ПК» перепутал правильное написание каталога и написал «/c/Users/harih» вместо...",
    "mainImageUrl": null,
    "amp": true,
    "customTrackerLinks": []
  },
  "polls": [],
  "commentsEnabled": {
    "status": true,
    "reason": null
  },
  "rulesRemindEnabled": false,
  "votesEnabled": true,
  "status": "published",
  "plannedPublishTime": null,
  "isSeo": false,
  "checked": null,
  "hasPinnedComments": false,
  "format": null,
  "banner": null,
  "multiwidget": null,
  "multiwidgetUuid": null,
  "readingTime": 2,
  "complexity": null,
  "isEditorial": true,
  "flowNew": {
    "id": "8",
    "title": "Системное администрирование",
    "alias": "admin"
  },
  "linkedPostTranslation": null,
  "hasRegionalRestrictions": false
}
```
