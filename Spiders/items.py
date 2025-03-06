# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class articleItem(Item):
    title = Field()
    updated = Field()
    content = Field()
    link = Field()
    
    def to_dict(self):
        return {
            'updated': self['updated'].strftime("%d/%m/%Y"),
            'title': self['title'],
            'content': self['content'],
            'link': self['link'],
        }