#!/usr/bin/python
# -*- coding: utf-8 -*-

import flickr_api
import MySQLdb as mdb
import logging
import sys
import os
import re
import shutil
import codecs
import urllib
import pyexiv2
import datetime
import ConfigParser
from PIL import Image
from operator import attrgetter
from dateutil import parser
from collections import defaultdict
from httplib import IncompleteRead

logging.basicConfig(filename='connector.log',filemode='w',level=logging.DEBUG,format='%(asctime)s %(message)s')
logging.debug('Get user photos')

config = ConfigParser.RawConfigParser()
config.read('app.cfg')

api_key = config.get('Flickr API', 'key')
api_secret = config.get('Flickr API', 'secret')

flickr_api.set_keys(api_key, api_secret)

# Read authorization token from config file and write it to temporary auth file.
oauth_token = config.get('Flickr API', 'oauth_token')
with open("auth.cfg", "w") as authf:
    authf.write(oauth_token)

# Set auth token.
flickr_api.set_auth_handler('auth.cfg')
user = flickr_api.test.login()
# Remove auth file.
os.remove('auth.cfg')

logging.debug('Open database connection')

server = config.get('MySQL', 'server')
dbname = config.get('MySQL', 'dbname')
username = config.get('MySQL', 'username')
password = config.get('MySQL', 'password')

con = mdb.connect(server, username, password, dbname);
con.set_character_set('utf8')
cur = con.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
failedphoto = None

photorootdir = u'/siilo/users/ikudjoi/flickr/'

def escape_apostrophe(str):
    return str.replace('\'','\'\'')

def remove_extension(filename):
    lower = filename.lower()
    if lower.endswith('.jpeg'):
        return filename[:-5]
    if lower.endswith('.jpg') or lower.endswith('.mov') or lower.endswith('.avi'):
        return filename[:-4]
    return filename


def get_int_list_from_database(sql):
    result = []
    with con:
        cur.execute(sql)
        row = cur.fetchone()
        while row is not None:
            result.append(row[0])
            row = cur.fetchone()
    return result


def photo_entry(photo):
    try:
        return u"""%s, FROM_UNIXTIME(%s), \'%s\', \'%s\', %s, \'%s\', \'%s\', FROM_UNIXTIME(%s),
                \'%s\', \'%s\', \'%s\', \'%s\', %s, %s""" % \
            (photo.id, photo.dateupload, photo.originalformat, escape_apostrophe(remove_extension(photo.title)), photo.views, photo.datetaken, \
            escape_apostrophe(photo.description), photo.lastupdate, photo.url_o, photo.url_t, photo.url_s, photo.url_m, photo.o_width, photo.o_height)
    except (UnicodeEncodeError, TypeError):
        failedphoto = photo
        print photo.__dict__
        raise

#def retry_if_incompleteread(exception):
#    return isinstance(exception, IncompleteRead)

#@retry(stop_max_attempt_number = 5, retry_on_exception=retry_if_incompleteread)
def get_photos(user, page):
    logging.debug('Load photo information from Flickr.')
    return user.getPhotos(per_page=500, extras="""description,
            date_upload, date_taken, icon_server, original_format,
            last_update, geo, tags, machine_tags, o_dims, views, media,
            path_alias, url_o, url_t, url_s, url_m, o_dims""",page=page)        

def update_photos():
    page = 1
    photos = get_photos(user,page)
    with con:
        cur.execute(u'TRUNCATE TABLE f_photo;')
        while page <= photos.info.pages:
            logging.debug('Build command')
            
            cmd = u"""INSERT INTO f_photo (id, dateUploaded, originalFormat, title, views,
            taken, description, lastUpdate, urlOriginal, urlTiny, urlSmall, urlMedium, width, height)
                      VALUES (""" + u'),('.join([photo_entry(photo) for photo in photos]) + u');'
            logging.debug('Execute command')
            cur.execute(cmd)
            con.commit()
            logging.debug('Inserted %s photos to the database.' % len(photos))
            page+=1
            photos = get_photos(user,page)

def photoset_entry(photoset):
    return u'%s, \'%s\', FROM_UNIXTIME(%s), %s, %s, FROM_UNIXTIME(%s)' % \
           (photoset.id, photoset.title, photoset.date_create, photoset.count_comments, photoset.count_views, photoset.date_update)

def update_photosets():
    with con:
        # With this sql we get the date of the earliest photo per set.
        sql = """
        select ps.id, mintaken.prefix
        from f_photoset ps
        left join f_photosetnodate nd
        on ps.id = nd.id
        inner join (
        select psp.photosetid, date_format(min(fp.taken), '%Y-%m-%d') as prefix
        from f_photosetphoto psp
        inner join f_photo fp
        on psp.photoId = fp.id
        group by psp.photosetId) mintaken
        on ps.id = mintaken.photosetid
        where nd.id is null
        """
        cur.execute(sql)
        
        setdates = dict()
        row = cur.fetchone()
        while row is not None:
            setdates[str(row[0])] = row[1]
            row = cur.fetchone()

        photosets = user.getPhotosets()
        for ps in photosets:
            if ps.id in setdates:
                prefix = setdates[ps.id]
                if not ps.title.startswith(prefix):
                    title = ps.title
                    if (re.match('\d\d\d\d-\d\d-\d\d .*', ps.title)):
                        title = title[11:]
                    title = prefix + ' ' + title
                    logging.debug('Changing photoset %s title ''%s'' to ''%s''.' % (ps.id, ps.title, title))
                    #ps.editMeta(title = title)

        cur.execute('TRUNCATE TABLE f_photoset;')
        logging.debug('Build command')
        cmd = u'INSERT INTO f_photoset (id, title, createDate, commentCount, viewCount, updateDate) ' + \
              u'VALUES (' + u'),('.join([photoset_entry(photoset) for photoset in photosets]) + u');'
        logging.debug('Execute command')
        cur.execute(cmd)
        con.commit()
        logging.debug('Inserted %s photosets to the database.' % len(photosets))

        afile = codecs.open('albums.txt', 'w', 'utf-8')
        cur.execute('TRUNCATE TABLE f_photosetphoto;')
        photosetphotos = []
        for photoset in sorted(photosets, key=lambda ps: ps.title, reverse=True):

            line = u'<li><a href="https://www.flickr.com/photos/ilkkakudjoi/sets/%s/">%s</a></li>\n' % (photoset.id, photoset.title)
            afile.write(line)
            
            page = 1
            while True:
                photos = photoset.getPhotos(page=page)
                logging.debug('Retrieved %s photo ids from set %s.' % (len(photos), photoset.title))
                photosetphotos += [(photo.id, photoset.id) for photo in photos]
                page += 1
                if (page > photos.info.pages):
                    break
        afile.close()        
        cmd = u'INSERT INTO f_photosetphoto (photoId, photosetId) ' + \
              u'VALUES (' + u'),('.join([psp[0] + ', ' + psp[1] for psp in photosetphotos]) + u');'
        cur.execute(cmd)
        con.commit()
        logging.debug('Inserted %s photo-photoset links to the database.' % len(photosetphotos))
        
def delete_duplicates():

    logging.debug('Find duplicates by title and taken timestamp.')
    sql = """SELECT p.id FROM f_photo p
             INNER JOIN (SELECT MIN( id ) AS id, title, taken
             FROM f_photo
             GROUP BY taken, title HAVING COUNT( * ) > 1) d
             ON p.title = d.title AND p.taken = d.taken
             WHERE p.id <> d.id;"""
    
    duplicateids = get_int_list_from_database(sql)
    logging.debug('Found %s duplicates.' % len(duplicateids))

    for id in duplicateids:
        logging.debug('Deleting photo id %s.' % id)
        photo = flickr_api.Photo(id = id)
        photo.delete()


def order_photosets_by_taken_date():
    logging.debug('Get photoset ids ordered by photo taken date desc.')
    sql = """select ps.id
    from f_photoset ps
    inner join f_photosetphoto psp on ps.id = psp.photosetId
    left join f_photosetnodate nd on ps.id = nd.id
    inner join f_photo p on psp.photoId = p.id
    group by ps.id, ps.title order by max(case when nd.id is not null then cast('1900-01-01' as datetime) else p.taken end) desc"""

    setids = get_int_list_from_database(sql)
    setids = [str(i) for i in setids]
    logging.debug('Retrieved %s photoset ids.' % len(setids))
    flickr_api.Photoset().orderSets(photoset_ids=','.join(setids))

    logging.debug('Ordering photos inside sets. Getting list of sets.')
    photosets = user.getPhotosets()
    for photoset in photosets:
        logging.debug('Getting photos of set %s and ordering them by taken date.' % photoset.title)
        sql = """select p.id from f_photo p
                 inner join f_photosetphoto psp on p.id = psp.photoId
                 where psp.photosetId = %s
                 order by p.taken asc""" % photoset.id
        photoids = get_int_list_from_database(sql)
        photoids = [str(i) for i in photoids]
        photoset.reorderPhotos(photo_ids=','.join(photoids))

def get_local_sets_and_photos():
    setfolders = os.listdir(photorootdir)
    setfolders = [(int(parts[1][1]), parts[0]) for parts in [(folder, folder.split('_')) for folder in setfolders] if len(parts[1]) >= 3]
    if len(set([folder[0] for folder in setfolders])) != len(setfolders):
        raise Exception('Sets contain duplicates.')
    
    setidtoset = dict(setfolders)
    photos = [(folder, os.listdir(os.path.join(photorootdir,folder))) for folder in setidtoset.values()]
    photos = [(folder[0], [(photo[:-5].split('_'), photo) for photo in folder[1] if photo[-5:] == '.jpeg']) for folder in photos]
    photos = [(folder[0], [(int(photo[0][1]), photo[1]) for photo in folder[1] if len(photo[0]) == 2]) for folder in photos]
    photos = [[(photo[0], (folder[0], photo[1])) for photo in folder[1]] for folder in photos]
    photos = [photo for folder in photos for photo in folder]
    photoids = [photo[0] for photo in photos]
    photoidtophoto = defaultdict(list)
    for photo in photos:
        photoidtophoto[photo[0]].append(photo[1])

    return setidtoset, photoidtophoto

def download():
    setidtoset, photoidtophoto = get_local_sets_and_photos()
    
    photosets = user.getPhotosets()
    setsinflickr = []
    for photoset in photosets:
        setsinflickr.append(int(photoset.id))
        page = 1
        photos = photoset.getPhotos(per_page=500, extras='date_taken, url_o, o_dims',page=page)
        while page < photos.info.pages:
            page += 1
            photos += photoset.getPhotos(per_page=500, extras='date_taken, url_o, o_dims',page=page)
        photosetdate = min([photo.datetaken for photo in photos])
        photosetpath = '%s_%s_%s' % (parser.parse(photosetdate).strftime('%Y%m%d'), photoset.id, photoset.title.replace(' ', '_'))

        logging.debug(u'Ensuring that path "%s" exists.' % photosetpath)
        move = False
        if int(photoset.id) in setidtoset:
            currentsetpath = setidtoset[int(photoset.id)]
            if currentsetpath != photosetpath:
                move = True
                
        photosetpath = os.path.join(photorootdir, photosetpath)
        if move:
            currentsetpath = os.path.join(photorootdir, currentsetpath)
            logging.debug(u'Renaming set folder %s to "%s".' % (currentsetpath, photosetpath))
            os.rename(currentsetpath, photosetpath)
            setidtoset, photoidtophoto = get_local_sets_and_photos()
        elif not os.path.exists(photosetpath):
            logging.debug(u'Creating new photoset folder "%s".' % photosetpath)
            os.makedirs(photosetpath)
        else:
            logging.debug(u'Photoset folder "%s" already exists.' % photosetpath)
        
        alreadycheckedphotos = dict()
        for photo in photos:
            photopath = '%s_%s.jpeg' % (parser.parse(photo.datetaken).strftime('%Y%m%d%H%M%S'), photo.id)
            photopath = os.path.join(photosetpath, photopath)

            if photo.id in alreadycheckedphotos:
                os.link(alreadycheckedphotos[photo.id], photopath)
                continue

            elif int(photo.id) in photoidtophoto:
                currentphotopaths = photoidtophoto[int(photo.id)]
                currentphotopath = currentphotopaths[0]
                currentphotopath = os.path.join(photorootdir, currentphotopath[0], currentphotopath[1])
                delete = False
                try:
                    im = Image.open(currentphotopath)
                    im.load()
                    (width, height) = im.size
                    if ((int(photo.o_width) != int(width)) or (int(photo.o_height) != int(height))):
                        logging.debug(u'Found photo with unmatching size "%s" (local w%s x h%s, flickr w%s x h%s).' % \
                                      (currentphotopath, width, height, photo.o_width, photo.o_height))
                        delete = True
                except (IOError):
                    logging.debug(u'Found unreadable photo "%s".' % (currentphotopath))
                    delete = True
                if delete:
                    os.remove(currentphotopath)
                    # no continue here
                elif currentphotopath != photopath:
                    logging.debug(u'Moving photo "%s to "%s".' % (currentphotopath, photopath))
                    os.rename(currentphotopath, photopath)
                else:
                    logging.debug(u'Photo id %s already exists in path "%s".' % (photo.id, photopath))

                for extraphoto in currentphotopaths[1:]:
                    extraphotopath = os.path.join(photorootdir, extraphoto[0], extraphoto[1])
                    os.remove(extraphotopath)
                del photoidtophoto[int(photo.id)]

                alreadycheckedphotos[photo.id] = photopath
                if not delete:
                    continue

            logging.debug(u'Downloading photo id %s to path "%s".' % (photo.id, photopath))
            try:
                urllib.urlretrieve(photo.url_o, photopath)
            except (ContentTooShortError):
                logging.debug(u'Failed to download photo from path "%s". Trying again.' % (photo.url_o))
                urllib.urlretrieve(photo.url_o, photopath)
            except:
                logging.debug(u'Failed to download photo from path "%s".' % (photo.url_o))
                raise
            alreadycheckedphotos[photo.id] = photopath
                        
    logging.debug(u'Removing photos that no longer exists in Flickr.')
    for photoid in photoidtophoto:
        for photopath in photoidtophoto[photoid]:
            phototoremove = os.path.join(photorootdir, photopath[0], photopath[1])
            logging.debug(u'Removing photo %s.' % (phototoremove))
            os.remove(phototoremove)
    logging.debug(u'Removing photosets that no longer exists in Flickr.')
    for photosetid in setidtoset:
        if photosetid not in setsinflickr:
            settoremove = os.path.join(photorootdir, setidtoset[photosetid])
            logging.debug(u'Removing photoset %s.' % (settoremove))
            shutil.rmtree(settoremove)
             
def move_date_taken(sql, photoids, dtfix, giventime):
    if not sql is None:
        logging.debug('Getting photo ids from database.')
        photoids = get_int_list_from_database(sql)

    if not isinstance(photoids, list):
        photoids = photoids.split(',')

    photoids = [int(i) for i in photoids]
    setidtoset, photoidtophoto = get_local_sets_and_photos()
    # Ensure that all photos have been loaded by constructing a dictionary.

    photoidtolocalphoto = dict([[id, os.path.join(photorootdir, photoidtophoto[id][0][0], photoidtophoto[id][0][1])] for id in photoids])
    datekeys = ['Exif.Photo.DateTimeOriginal', 'Exif.Photo.DateTimeDigitized']
    photoidtodatetime = dict()
        
    for id in photoidtolocalphoto:
        metadata = pyexiv2.ImageMetadata(photoidtolocalphoto[id])
        metadata.read()
        for datekey in datekeys:
            if datekey in metadata.exif_keys:
                dt = metadata[datekey].value
                if id not in photoidtodatetime:
                    photoidtodatetime[id] = dt
                elif dt != photoidtodatetime[id]:
                    raise Exception('Different dates in photo.')
        if id not in photoidtodatetime:
            if giventime is not None:
                photoidtodatetime[id] = giventime
            else:
                raise 'No take date found from photo'
    for id in photoidtolocalphoto:
        logging.debug('Changing taken dates of photo %s %s.' % (id, photoidtolocalphoto[id]))
        metadata = pyexiv2.ImageMetadata(photoidtolocalphoto[id])
        metadata.read()

        dt = photoidtodatetime[id]
        if giventime is None:
            dt = dt + dtfix
        for datekey in datekeys:
            if datekey in metadata.exif_keys:
                metadata[datekey].value = dt
            else:
                metadata[datekey] = pyexiv2.ExifTag(datekey, dt)
            metadata.write()

    for id in photoidtolocalphoto:
        logging.debug('Replacing photo %s ''%s''.' % (id, photoidtolocalphoto[id]))
        flickr_api.Upload.replace(photo_id = id, async = False, photo_file = photoidtolocalphoto[id])
        
        
if ('photos' in sys.argv):
    update_photos()
if ('sets' in sys.argv):
    update_photosets()
if ('dedupr' in sys.argv):
    delete_duplicates()
if ('order' in sys.argv):
    order_photosets_by_taken_date()
if ('download' in sys.argv):
    download()

if ('move' in sys.argv):
    sql = """select p.id
    from f_photo p
    inner join f_photosetphoto psp
    on p.id = psp.photoid
    where psp.photosetid = 72157646713519388
    and left(p.title,3)='DSC'"""
    
    #move_date_taken(None, '15157778368', datetime.timedelta(minutes=38), None)
    #move_date_taken(None, '14923073599,14923125960', None, None, None, None, datetime.datetime(2014, 8, 16, 13, 15))
