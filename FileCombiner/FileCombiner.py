#!/usr/bin/env python

# The MIT License (MIT)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# @author Stephen J. Lee
# Given two binary files, open both and combine

import os
import sys
import datetime
import argparse
import time

class Directory (object):

    def __init__(self, directory):
        self.directory = directory
        self.filePathSeparator = os.sep

    def makeDir(self):
        try:
            # Create target Directory and parent structure
            os.makedirs(self.directory)
            return self.directory
        except:
            return self.directory

    def getExtFiles (self, root, name, nameLen, fileExt):
        if nameLen == 0:
            return []
        elif type(fileExt) == bool and fileExt == True:
            return [os.path.join(root,name)]
        elif type(fileExt) == str and name.split('.')[-1] in fileExt:
            return [os.path.join(root,name)]
        else: return []

    def getFilePaths(self, recursive=False, fileExt=True):
        self.files = []
        if recursive == True:
            self.tree = [i for i in os.walk(self.directory)]
            # i = (path,dir,files)
            # j = file
            for i in self.tree:
                for j in i[2]:
                    self.files+=self.getExtFiles(i[0],j,[j],fileExt)
        else:
            self.tree = [os.walk(self.directory).next()]
            for j in self.tree[0][2]:
                self.files+=self.getExtFiles(self.tree[0][0],j,[j],fileExt)
        return self.files

    def makeFile(self,fileName):
        # this can me passed a file name or file path
        if self.filePathSeparator not in fileName:
            # this is a file name
            fpath = os.path.join(self.directory,fileName)
        else:
            # this is a file path
            fpath = fileName
        return fpath

    def pullFile(self,filePath):
        fn = filePath.split(self.filePathSeparator)[-1]
        os.rename(filePath,os.path.join(self.directory,fn))
        return


class SourDir(Directory):
    # This will handle the Source directory for files
    pass


class DestDir(Directory):
    # This will handle the Final Destination directory for
    # post processed files
    pass


class TempDir(Directory):
    # If an external process is grabbing post processed data,
    # this allows the data to write in peace. Hidden file iterations
    # caused errors since processes grabbed ALL files
    def __init__(self, directory):
        self.tempDir = os.path.join(directory, 'temp')
        Directory.__init__(self,self.tempDir)

    def makeFileName(self, timeName, destinationFileName, fileExt, sourceDir):
        prefix = ''
        suffix = fileExt
        if timeName == True:
            prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        if destinationFileName is None:
            destinationFileName = sourceDir.split(self.filePathSeparator)[-1]
        if type(fileExt) is not str:
            suffix = ''
        elif suffix[0] != '.':
            suffix = '.'+fileExt
        prefix += destinationFileName
        self.desFileName = prefix + suffix
        return


class SharDir(Directory):
    # This directory will hold the pre-processed raw data
    def __init__(self,directory, sharDir):
        self.sharDir = sharDir
        if self.sharDir in ['', True]:
            self.sharDir = 'processed'
        self.newDirectory = os.path.join(directory, self.sharDir)
        Directory.__init__(self, self.newDirectory)

class Package(object):
    def __init__(self,**kwargs):
        '''fileExt: desired file types, if set to true, get all
        recursiveSearch is asking to look in subfolders if True'''
        default_attr = {'sourceDir':'', 'fileExt':True, 'recurSearch':False,
                        'destinationDir':'', 'destinationFileName':None,
                         'sizeLimit':2, 'sizeUnit':"GB",
                        'temporaryDir':'', 'timeName':True,
                        'shardFileDir':'', 'deleteFile':False}
        default_attr.update(kwargs)
        self.__dict__.update(default_attr)
        # sourceFiles in sourceDir are files to be combined
        self.sd = SourDir(self.sourceDir)
        self.sourceDir = self.sd.makeDir()
        self.sourceFiles = self.sd.getFilePaths(self.recurSearch, self.fileExt)
        # destDir is the place to put combined files
        self.dd = DestDir(self.destinationDir)
        self.destDir = self.dd.makeDir()
        # temp directory helps process write in peace
        self.td = TempDir(self.temporaryDir)
        self.tempDir = self.td.makeDir()
        # shardDir will move original files to another directory
        if self.shardFileDir is not False:
            self.shd = SharDir(self.destinationDir, self.shardFileDir)
            self.sharDir = self.shd.makeDir()
        # max size of the packet files
        self.sizeLimit = self.convert2Bytes(self.sizeLimit, self.sizeUnit)
        return

    def convert2Bytes(self,size,unit):
        bit_shift = {"B": 0, "kb": 7, "KB": 10, "mb": 17, "MB": 20,
                     "gb": 27, "GB": 30, "TB": 40, }
        return size*(1<<bit_shift[unit])

    def pack(self):
        self.td.makeFileName(self.timeName, self.destinationFileName,
                             self.fileExt, self.sourceDir)
        self.desFile = self.td.makeFile(self.td.desFileName)
        print (self.desFile)

        obj = open(self.desFile, 'ab+')
        # Combine all files into a larger file, based on preset size
        written = []
        for fn in self.sourceFiles:
            if os.path.getsize(self.desFile) < self.sizeLimit:
                # write to file
                with open(fn,'rb') as sFile:
                    obj.write(sFile.read())
                written.append(fn)
        # What to do with the original files
        if self.shardFileDir is not False:
            for i in written:
                self.shd.pullFile(i)
        if self.deleteFile is not False:
            for i in written:
                os.remove(fn)
        # Figure out what's left to do
        self.sourceFiles = list(set(self.sourceFiles)-set(written))
        # Move post process out of temp
        obj.close()
        self.dd.pullFile(self.desFile)
        return


    def packItAll(self):
        while len(self.sourceFiles) > 0:
            self.pack()
        os.rmdir(self.tempDir)
        return

class CustomArgs (object):
    def __init__(self, **args):
        self.args = args
        self.rosetta = {True:['true','yes'],
                          False: ['false','no'],
                          None : ['none', 'null']}
    def reverse(self):
        # reverses a simple dictionary list as values struct
        new_dict = {}
        for i in self.rosetta.items():
            for j in i[1]:
                 new_dict[j] = i[0]
        self.rosetta = new_dict
        return

    def interpret(self):
        # this is meant to interpret _true in case only strings
        # are the only thing that can be passed
        self.reverse()
        transDict = {}
        for k in self.args.keys():
            if str(self.args[k]).startswith('_'):
                try:
                    transDict[k] = self.rosetta[self.args[k][1:].lower()]
                except:
                    pass
            else:
                transDict[k] = self.args[k]
        return transDict




def main(argv):

    # When running from terminal, use the following arguements
    parser = argparse.ArgumentParser(description='Given files, opens and combines')
    parser.add_argument('--sourceDir', required=True,
                        help='folder with all files to combine')
    parser.add_argument('--destinationDir', required=True,
                        help='directory to put post processed combined files')
    parser.add_argument('--fileExt', required=True,
                        help='file ext,e.g csv,gz. True means any file types')
    parser.add_argument('--destinationFileName', default=None,
                        help='if None, then source folder name, else str name')
    parser.add_argument('--recurSearch', default=False,
                        help='True goes to subfolders')
    parser.add_argument('--sizeLimit', default=2, type=int,
                        help='size limit of resulting file')
    parser.add_argument('--sizeUnit', default= 'GB',
                        help='size unit of size limit of resulting file')
    parser.add_argument('--timeName', default=True,
                        help='True writes date time to control for version in '
                             'destination file name')
    parser.add_argument('--deleteFile', default=False,
                        help='Deletes Source File after write, be careful!')
    parser.add_argument('--shardFileDir', default=False,
                        help='Moves written source files to separate'
                             'destination sub folder False will not move it'
                             "and '' will create a subfolder called processed")
    parser.add_argument('--temporaryDir', default='',
                        help = 'This will create a temporary folder to dump'
                               'the results into. Useful when there is an'
                               'external process moving files preemptively')

    args = parser.parse_args(argv[1:])
    a = CustomArgs(**vars(args))
    args = a.interpret()

    p = Package(**args)
    p.packItAll()
    return

if __name__ == '__main__':
    main(sys.argv)