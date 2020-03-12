#!/usr/bin/python
# -*- coding: utf-8 -*-

from flickr import Flickr
from database import Database
import argparse
import logging
import os
import shutil
import requests
import datetime
from PIL import Image
from collections import defaultdict


def parse_arguments():
    """
    Parse command line arguments
    :return: argparse.ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="Flickr Organizer!")
    parser.add_argument('--photos', action='store_true', help="Update photos table")
    parser.add_argument('--sets', action='store_true', help="Update photo sets table")
    parser.add_argument('--ignore-photo-in-many-sets', action = 'store_true', help = "Do not issue a warning if a photo belongs to more than one set.")
    parser.add_argument('--ignore-photo-in-no-sets', action = 'store_true', help = "Do not issue a warning if a photo doesn't belongs to any set.")
    parser.add_argument('--delete-duplicates', action = 'store_true', help = "Delete duplicate photos.")
    parser.add_argument('--dry-run', action = 'store_true', help = "Combine with --delete-duplicates, will only show what would be done.")
    parser.add_argument('--order-sets', action = 'store_true', help = "Reorder sets by taken date")
    parser.add_argument('--download', type = str, default = "siilo", help = "Download all photos from the Flickr Account to local directory")
    return parser.parse_args()

logging.basicConfig(filename='connector.log',filemode='w',level=logging.DEBUG,format='%(asctime)s %(message)s')
logging.debug('Get user photos')

db = Database()
flickr = Flickr(db)

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

        
def delete_duplicates():

    logging.debug('Find duplicates by title and taken timestamp.')
    sql = """SELECT p.id FROM f_photo p
             INNER JOIN (SELECT MIN( id ) AS id, title, taken
             FROM f_photo
             GROUP BY taken, title HAVING COUNT( * ) > 1) d
             ON p.title = d.title AND p.taken = d.taken
             WHERE p.id <> d.id;"""
    
    duplicateids = db.get_int_list_from_database(sql)
    logging.debug('Found %s duplicates.' % len(duplicateids))

    for id in duplicateids:
        logging.debug('Deleting photo id %s.' % id)
        photo = flickr.Photo(id = id)
        photo.delete()


def order_photosets_by_taken_date():
    logging.debug('Get photoset ids ordered by photo taken date desc.')
    sql = """select ps.id
    from f_photoset ps
    inner join f_photosetphoto psp on ps.id = psp.photosetId
    left join f_photosetnodate nd on ps.id = nd.id
    inner join f_photo p on psp.photoId = p.id
    group by ps.id, ps.title order by max(case when nd.id is not null then cast('1900-01-01' as datetime) else p.taken end) desc"""

    setids = db.get_int_list_from_database(sql)
    setids = [str(i) for i in setids]
    logging.debug('Retrieved %s photoset ids.' % len(setids))
    flickr.Photoset().orderSets(photoset_ids=','.join(setids))

    logging.debug('Ordering photos inside sets. Getting list of sets.')
    photosets = flickr.get_photosets()
    for photoset in photosets:
        logging.debug('Getting photos of set %s and ordering them by taken date.' % photoset.title)
        sql = """select p.id from f_photo p
                 inner join f_photosetphoto psp on p.id = psp.photoId
                 where psp.photosetId = %s
                 order by p.taken asc""" % photoset.id
        photoids = db.get_int_list_from_database(sql)
        photoids = [str(i) for i in photoids]
        try:
            photoset.reorderPhotos(photo_ids=','.join(photoids))
        except flickr.FlickrError as e:
            logging.warn("Could not reorder photoset %s.", photoset.id)

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

def download_file(url, file_name):
    with open(file_name, "wb") as file:
        # get request
        response = requests.get(url)
        # write to file
        file.write(response.content)

def download():
    setidtoset, photoidtophoto = get_local_sets_and_photos()
    
    photosets = flickr.get_photosets()
    setsinflickr = []
    for photoset in photosets:
        setsinflickr.append(int(photoset.id))
        page = 1
        photos = photoset.getPhotos(per_page=500, extras='date_taken, url_o, o_dims',page=page)
        while page < photos.info.pages:
            page += 1
            photos += photoset.getPhotos(per_page=500, extras='date_taken, url_o, o_dims',page=page)
        photosetdate = min([photo.datetaken for photo in photos])
        photosetpath = '%s_%s_%s' % (datetime.datetime.strptime(photosetdate).strftime('%Y%m%d'), photoset.id, photoset.title.replace(' ', '_'))

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
            photopath = '%s_%s.jpeg' % (datetime.datetime.strptime(photo.datetaken).strftime('%Y%m%d%H%M%S'), photo.id)
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
                download_file(photo.url_o, photopath)
            except (Exception):
                logging.debug(u'Failed to download photo from path "%s". Trying again.' % (photo.url_o))
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
        photoids = db.get_int_list_from_database(sql)

    if not isinstance(photoids, list):
        photoids = photoids.split(',')

    photoids = [int(i) for i in photoids]
    setidtoset, photoidtophoto = get_local_sets_and_photos()
    # Ensure that all photos have been loaded by constructing a dictionary.

    photoidtolocalphoto = dict([[id, os.path.join(photorootdir, photoidtophoto[id][0][0], photoidtophoto[id][0][1])] for id in photoids])
    datekeys = ['Exif.Photo.DateTimeOriginal', 'Exif.Photo.DateTimeDigitized']
    photoidtodatetime = dict()
        
    for id in photoidtolocalphoto:
        img = Image.open(photoidtolocalphoto[id])
        metadata = img._getexif()
        #metadata.read()
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
                raise('No take date found from photo')
    for id in photoidtolocalphoto:
        logging.debug('Changing taken dates of photo %s %s.' % (id, photoidtolocalphoto[id]))
        img = Image.open(photoidtolocalphoto[id])
        metadata = img._getexif()

        dt = photoidtodatetime[id]
        if giventime is None:
            dt = dt + dtfix
        for datekey in datekeys:
            if datekey in metadata.exif_keys:
                metadata[datekey].value = dt
            else:
                raise NotImplementedError("Broken in Python 3 conversion!")
            metadata.write()

    for id in photoidtolocalphoto:
        logging.debug('Replacing photo %s ''%s''.' % (id, photoidtolocalphoto[id]))
        flickr.Upload.replace(photo_id = id, photo_file = photoidtolocalphoto[id])

arguments = parse_arguments()

if arguments.sets:
    flickr.update_photosets()
if arguments.photos:
    ignore_photo_in_many_sets = arguments.ignore_photo_in_many_sets
    ignore_photo_in_no_sets = arguments.ignore_photo_in_no_sets
    flickr.update_photos(ignore_photo_in_many_sets, ignore_photo_in_no_sets)
if arguments.delete_duplicates:
    dry_run = arguments.dry_run
    flickr.delete_duplicates(dry_run)
if arguments.order_sets:
    dry_run = arguments.dry_run
    flickr.fix_ordering_of_sets(dry_run)
#if ('order' in sys.argv):
#    order_photosets_by_taken_date()
#if ('download' in sys.argv):
#    download()

# if ('move' in sys.argv):
#     sql = """select p.id
#     from f_photo p
#     inner join f_photosetphoto psp
#     on p.id = psp.photoid
#     where psp.photosetid = 72157646713519388
#     and left(p.title,3)='DSC'"""
#
    #move_date_taken(None, '15157778368', datetime.timedelta(minutes=38), None)
    #move_date_taken(None, '14923073599,14923125960', None, None, None, None, datetime.datetime(2014, 8, 16, 13, 15))
