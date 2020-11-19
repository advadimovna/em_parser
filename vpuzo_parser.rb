require 'nokogiri'
require 'sqlite3'
require 'time'
require 'eventmachine'
require 'em-http-request'
require 'byebug'
require 'open-uri'
t = Time.now
site = ('https://vpuzo.com/')
puts "Site #{site}"

start_page = "https://vpuzo.com"
puts "Start page #{start_page}"
parsed_start_page=Nokogiri::HTML(open(start_page))
pages_count=parsed_start_page.css('li:nth-child(6) .b3').text.to_i
puts "Finded #{pages_count} pages"
urls = []
(1..500).each do |p|
  url = "#{start_page}/page/#{p.to_s}/"
  urls << url
end
concurrency = 32
items = []
EM.run do
  EM::Iterator.new(urls, concurrency).each(
      proc { |url, iter|
        http = EventMachine::HttpRequest.new(url, ssl: {verify_peer: false}, :connect_timeout => 10).get
        http.callback do |response|
          document = Nokogiri::HTML(response.response)
          document.xpath('//*/div[@class="catalog-box"]/a/@href').each do |item|
            items << item
          end
          iter.next
        end
        http.errback do
          p "Failed: #{url}"
          iter.next
        end
      },
      proc {
        puts "#{urls.length} pages parsed"
        EM.stop
      })
end
"Start parsing recipes"
item_links = []
items.map do |item|
  item_links << item.to_s
end
puts "Finded #{item_links.length} recipes"
recipes = []
EM.run do
  EM::Iterator.new(item_links, concurrency).each(
      proc { |url, iter|
        http = EventMachine::HttpRequest.new(url, ssl: {verify_peer: false}, :connect_timeout => 20).get
        http.callback do |response|
          document = Nokogiri::HTML(response.response)
          recipe = {
              :name => document.css('h1').text,
              :image => "#{start_page}#{document.xpath('//div[@class="recipe-main-image"]//img/@src')}".to_s,
              :author => document.xpath('//span[@itemprop="author"]/text()').to_s,
              :author_rating => document.css('.recipe-author-rating span').text.delete(' ').to_f,
              :author_link => document.xpath('//span[@class="h4"]/a/@href').text,
              :cook_time => document.css('.info-time time').text,
              :geography => document.css('.info-location a').text,
              :main_ingredient => document.css('.info-ingredient a').text,
              :type => document.css('.info-type-salad a').text,
              :link => url
          }
          recipes << recipe
          iter.next
        end
        http.errback do
          p "Failed: #{url}"
          iter.next
        end
      },
      proc {
        puts "#{recipes.length} recipes parsed"
        EM.stop
      })
end
#Вывести содержимое хэша с рецептами в столбик
# recipes.each do |recipe|
#   recipe.each do |key, value|
#   puts "#{key}:#{value}"
#   end
# end

puts "Time for downloading and parsing #{(t - Time.now).abs.round(2)} sec"
t = Time.now
puts "Writing in db"
db = SQLite3::Database.open("recipes.db")
db.transaction do |db|
  db.execute("CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name text, rating float,
                link text, created_at datetime, updated_at datetime)")
  db.execute("CREATE TABLE IF NOT EXISTS recipes_natural (id INTEGER PRIMARY KEY NOT NULL, name text, author text, image text,
                cook_time text, geography text, main_ingredient text, dish_type text, link text,
                created_at datetime, updated_at datetime)")
  db.execute("CREATE TABLE IF NOT EXISTS recipes (id INTEGER PRIMARY KEY NOT NULL, name text, author_id integer, image text,
                cook_time text, geography text, main_ingredient text, dish_type text, link text,
                created_at datetime, updated_at datetime, FOREIGN KEY (author_id) REFERENCES authors(id))")
  db.execute("DELETE FROM authors;
              DELETE FROM recipes_natural")
  db.execute("DELETE FROM recipes")
  uniq_authors=recipes.uniq {|a| a[:author]}
  uniq_authors.each do |author|
    db.execute("INSERT INTO authors (name, rating, link, created_at)
                VALUES (?, ?, ?, DATETIME('now'))",
               author[:author], author[:author_rating], author[:author_link])
  end
  recipes.each do |recipe|
    db.execute("INSERT INTO recipes_natural (name, image, author, cook_time, geography, main_ingredient, dish_type, link, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))",
               recipe[:name], recipe[:image], recipe[:author], recipe[:cook_time], recipe[:geography],
               recipe[:main_ingredient], recipe[:type], recipe[:link],)
  end
  db.execute("INSERT INTO recipes (id, name, image, author_id, cook_time, geography, main_ingredient, dish_type, link, created_at)
              SELECT recipes_natural.id, recipes_natural.name, image, authors.id, cook_time, geography, main_ingredient, dish_type,
              recipes_natural.link, recipes_natural.created_at
              FROM recipes_natural INNER JOIN authors WHERE authors.name=recipes_natural.author")
  db.execute("DROP TABLE recipes_natural")
  # db.execute("ALTER TABLE recipes ADD COLUMN author_id integer REFERENCES authors(id)")
  # db.execute("UPDATE recipes SET author_id = (SELECT authors.id FROM authors WHERE authors.name=recipes.author) WHERE author_id IS NULL")
  # db.execute("ALTER TABLE recipes ADD CONSTRAINT author_fk FOREIGN KEY (author_id) REFERENCES authors(id)")
end
db.close
puts "DB done!"
puts "Time for db inserting #{(t - Time.now).abs.round(2)} sec"

#rails generate scaffold Author name:string rating:float link:string
#rails generate scaffold Recipe name:string author:string image:string cook_time:string geography:string main_ingredient:string type:string link:string