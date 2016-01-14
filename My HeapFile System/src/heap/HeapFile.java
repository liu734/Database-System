package heap;
import global.Page;
import global.PageId;
import global.RID;
import chainexception.ChainException;

/**

   * If the given name already denotes a file, this opens it; otherwise, this

   * creates a new empty file. A null name produces a temporary heap file which

   * requires no DB entry.

   */
public class HeapFile{
String name;

  public HeapFile(String name){
        boolean newlyCreated = false;
        boolean pinned = false;
        boolean succeed = false;


              // See if the heapfile already exists
                this.name = new String(name);
                PageId headerPageId = null;
                try {
                        try {
                                if((headerPageId =global.Minibase.DiskManager.get_file_entry(name)) != null)
                                        return;
                               
                                // Not existing, create a new file
                               	    	headerPageId =	global.Minibase.DiskManager.allocate_page(1);
                                global.Minibase.DiskManager.add_file_entry(name, headerPageId);
                        } catch(Exception e) {
                        }
                       
                        newlyCreated = true;
                       
                        Page headerPage = new Page();
                        try {
                                // Initialize the first page
                               global.Minibase.BufferManager.pinPage(headerPageId, headerPage, false);
                        } catch(Exception e) {
                        }
                       
                        pinned = true;
                       
                        HFPage hfPage = new HFPage(headerPage);
                        try {
                              //  hfPage.init(headerPageId, headerPage);
                                hfPage.setCurPage(headerPageId);
                                hfPage.setNextPage(headerPageId);
                                hfPage.setPrevPage(headerPageId);
                        } catch(Exception e) {

                        }
                       
                        succeed = true;
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(pinned) {
                                        global.Minibase.BufferManager.unpinPage(headerPageId, true);
                                }
                        } catch(Exception e) {
    
                        }
                        finally {
                                // In case anything bad happens, delete the heapfile if it is newly created
                                // no matter if the unpin is successful or not, we'll try delete the file
                                if((newlyCreated) && (!succeed)) {
                                        try {
                                                global.Minibase.DiskManager.delete_file_entry(name);
                                                global.Minibase.DiskManager.deallocate_page(headerPageId);
                                        }
                                        catch(Exception e) {
  
                                        }
                                }
                        }
                }




  }

















/**

   * Deletes the heap file from the database, freeing all of its pages.

   */

  public void deleteFile() {}

 

  /**

   * Inserts a new record into the file and returns its RID.

   *

   * @throws IllegalArgumentException if the record is too large

   */

  public RID insertRecord(byte[] record) throws HeapFileException, InvalidUpdateException, SpaceNotAvailableException {
	 PageId curPageId = new PageId();
                PageId nextPageId = new PageId();
                PageId headerPageId = null;
                Page curPage = new Page();
                HFPage hfPage = null;
                RID recRID = null;
                boolean curPagePinned = false;
                boolean curPageDirty = false;
                boolean newPageAllocated = false;
                boolean succeed = false;

 if (record.length > global.GlobalConst.PAGE_SIZE) {
                        throw new SpaceNotAvailableException(null, "record larger than page size");
                }






	 try {
                        try {
                                headerPageId = global.Minibase.DiskManager.get_file_entry(name);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "insertRecord() Failed");
                        }
                       
                        if (headerPageId == null)
                                throw new HeapFileException(null, "The heapfile has been deleted already.");
                       
                        curPageId.copyPageId(headerPageId);
                        // Try find a existing page to insert the Record
                        do {
                                // Pin current page
                                try {
                                        global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "insertRecord() Failed");
                                }
                                curPagePinned = true;

                                // try insert the records in current page
                                hfPage = new HFPage(curPage);
                                try {
                                        recRID = hfPage.insertRecord(record);
                                       
                                        // If we succeed insert a record in current page, just return.
                                        // The "finally statement will unpin the current page.
                                        if(recRID != null) {
                                                succeed = true;
                                                curPageDirty = true;
                                                return recRID;
                                        }
                                       
                                        nextPageId.copyPageId(hfPage.getNextPage());
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "insertRecord() Failed");
                                }

                                // Unpin current page and go next
                                try {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "insertRecord() Failed");
                                }
                                curPagePinned = false;
                                curPageId.copyPageId(nextPageId);
                        } while (curPageId.pid != headerPageId.pid);
                       
                        // OK, we could not find a existing page to insert this.
                        // Try allocate a new page and insert the record in the new page.
                        try {
                                curPageId = global.Minibase.DiskManager.allocate_page(1);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "insertRecord() Failed");
                        }
                        newPageAllocated = true;
                       
                        try {
                                global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "insertRecord() Failed");
                        }
                        curPagePinned = true;
                       
                        hfPage = new HFPage(curPage);
                        try {
                                //hfPage.init(curPageId, curPage);
                                hfPage.setCurPage(curPageId);
                                recRID = hfPage.insertRecord(record);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "insertRecord() Failed");
                        }
                       
                        // This insert should not fail, but if it does
                        // trigger an space not available exception
                        if(recRID == null)
                                throw new SpaceNotAvailableException(null, "failed to insert record in a new page");
                       
                        // Link the new page into heapfile
                        // rollback is done if anything bad happens
                        try{
			linkHFPage(headerPageId, curPageId, hfPage);
                       }
		      catch (Exception e){
		      }

                        curPageDirty = true;
                        succeed = true;
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(curPagePinned) {
                                        global.Minibase.BufferManager.unpinPage(curPageId, curPageDirty);
                                }
                        } catch (Exception e) {
                                throw new HeapFileException(e, "insertRecord() Failed and page not unpinned");
                        }
                        finally {
                                // In case anything bad happens, free the new page if it is newly created
                                // no matter if the unpin is successful or not, we'll try free the page
                                if((newPageAllocated) && (!succeed)) {
                                        try {
                                                global.Minibase.BufferManager.freePage(curPageId);
                                        }
                                        catch(Exception e) {
                                                throw new HeapFileException(e, "insertRecord() Failed and new page not free");
                                        }
                                }
                        }
                }
               
                return recRID;
  }


  /**

   * Reads a record from the file, given its id.

   *

   * @throws IllegalArgumentException if the rid is invalid

   */

  public byte[] selectRecord(RID rid) {
    return null;
  }

 

/**

   * Updates the specified record in the heap file.

   *

   * @throws IllegalArgumentException if the rid or new record is invalid

   */

  public boolean updateRecord(RID rid, Tuple newRecord) throws HeapFileException, InvalidUpdateException {


     PageId curPageId = new PageId();
                PageId nextPageId = new PageId();
                PageId headerPageId = null;
                Page curPage = new Page();
                HFPage hfPage = null;
                boolean curPagePinned = false;
                boolean curPageDirty = false;
                try {
                        try {
                                headerPageId = global.Minibase.DiskManager.get_file_entry(name);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "updateRecord() Failed");
                        }
                       
                        if (headerPageId == null)
                                throw new HeapFileException(null, "The heapfile has been deleted already.");
                       
                        curPageId.copyPageId(headerPageId);
                        // Try find a existing page to update the Record
                        do {
                                // Pin current page
                                try {
                                        global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "updateRecord() Failed");
                                }
                                curPagePinned = true;

                                // try update the record in current page
                                hfPage = new HFPage(curPage);
                                try {
                                        if(curPageId.pid == rid.pageno.pid) {
                                                Tuple oldTuple = newRecord;//hfPage.nextRecord(rid);
                                               
                                                if(oldTuple.getLength() != newRecord.getLength()) {
                                                        throw new InvalidUpdateException(null,
                                                                        "length of new tuple does not match old tuple");
                                                }
                                               
                                                oldTuple = newRecord; //oldTuple.tupleCopy(newtuple);
                                                curPageDirty = true;
                                                return true;
                                        }
                                       
                                        nextPageId.copyPageId(hfPage.getNextPage());
                               } /*catch (Exception e) {
                                        throw new HeapFileException(e, "updateRecord() Failed");
                                } catch (InvalidSlotNumberException e) {
                                       throw e;
                                }*/
				 catch (Exception e) {
                                        throw new InvalidUpdateException(e, "updateRecord() Failed");
                                }

                                // Unpin current page and go next
                                try {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "updateRecord() Failed");
                                }
                                curPagePinned = false;
                                curPageId.copyPageId(nextPageId);
                        } while (curPageId.pid != headerPageId.pid);
                       
                        // OK, we could not find a existing page containing this.
                        // Just return false.
                        return false;
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(curPagePinned) {
                                        global.Minibase.BufferManager.unpinPage(curPageId, curPageDirty);
                                }
                        } catch (Exception e) {
                                throw new HeapFileException(e, "updateRecord() Failed and page not unpinned");
                        }
                }

  }

 

  /**

   * Deletes the specified record from the heap file.

   *


   * @throws IllegalArgumentException if the rid is invalid

   */

  public boolean deleteRecord(RID rid)  throws ChainException {
                boolean found = false;
               
                PageId curPageId = new PageId();
                PageId nextPageId = new PageId();
                PageId headerPageId = null;
                Page curPage = new Page();
                HFPage hfPage = null;
                boolean curPagePinned = false;
                boolean curPageDirty = false;
                try {
                        try {
                                headerPageId = global.Minibase.DiskManager.get_file_entry(name);
                        } catch (Exception e) {

                        }
                       
                        if (headerPageId == null)
                                throw new ChainException (null, "header is null");
                       
                        curPageId.copyPageId(headerPageId);
                        // Try find a existing page to update the Record
                        do {
                                // Pin current page
                                try {
                                       global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                                } catch (Exception e) {

                                }
                                curPagePinned = true;

                                // try delete the record in current page
                                hfPage = new HFPage(curPage);
                                try {
                                        if(curPageId.pid == rid.pageno.pid) {
                                                hfPage.deleteRecord(rid);
                                               
                                                // Succeed delete a record in current page
                                                curPageDirty = true;
                                                found = true;
                                                break;
                                        }
                                       
                                        nextPageId.copyPageId(hfPage.getNextPage());
                                } catch (Exception e) {

                                } 

                                // Unpin current page and go next
                                try {
                                         global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {

                                }
                                curPagePinned = false;
                                curPageId.copyPageId(nextPageId);
                        } while (curPageId.pid != headerPageId.pid);
                       
                        // OK, we could not find a existing page containing this.
                        // Just return false.
                        if(!found) {
                                return false;
                        }
                       
                        // Delete succeed and current page is pinned.
                        // Unlink current page if its empty
                        hfPage = new HFPage(curPage);
                        boolean pageEmpty = false;
                        try {
                                pageEmpty = (hfPage.firstRecord()==null);
                        } catch (Exception e) {
                           
                        }
                       
                        if(pageEmpty) {
                                if (unlinkHFPage(headerPageId, curPageId, hfPage)) {
                                        // deallocate current page if unlink is successful
                                        try {
                                                 global.Minibase.BufferManager.freePage(curPageId);
                                        } catch (Exception e) {

                                        }
                                }
                        }
                       
                        return true;
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(curPagePinned) {
                                         global.Minibase.BufferManager.unpinPage(curPageId, curPageDirty);
                                }
                        } catch (Exception e) {

                        }
                }


  }

 

/**
SystemDefs.JavabaseDB
   * Gets the number of records in the file.

   */

  public int getRecCnt() {
	              // To make sure the count is correct even when multiple instance of the same file is open
                // We'll iterate the HFPages to count.
                // An alternative method is to store the record count in the header page, but in this way
                // the header page would be wasted for not being able to store records.
                int recCnt = 0;
                PageId curPageId = new PageId();
                PageId nextPageId = new PageId();
                PageId headerPageId = null;
                Page curPage = new Page();
                HFPage hfPage = null;
                boolean curPagePinned = false;
                try {
                        try {
                                headerPageId =global.Minibase.DiskManager.get_file_entry(name);
                        } catch (Exception e) {
                        }
                       
                        if (headerPageId == null)

                       
                        curPageId.copyPageId(headerPageId);
                        do {
                                // Pin current page
                                try {
                                        global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                                } catch (Exception e) {
                                }
                                curPagePinned = true;

                                // Count the records in current page
                                hfPage = new HFPage(curPage);
                                try {
                                        RID curRID = hfPage.firstRecord();
                                        while(curRID != null) {
                                                recCnt ++;
                                                curRID = hfPage.nextRecord(curRID);
                                        }
                                        nextPageId.copyPageId(hfPage.getNextPage());
                                } catch (Exception e) {
        
                                }

                                // Unpin current page and go next
                                try {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {
                                }
                                curPagePinned = false;
                                curPageId.copyPageId(nextPageId);
                        } while (curPageId.pid != headerPageId.pid);
                }
                finally {
                        // unpin current page if it is not unpinned yet and something bad happened
                        if(curPagePinned) {
                                try {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {
                                }
                        }
                }
               
                return recCnt;




  }

  public Tuple getRecord(RID rid) throws HeapFileException, InvalidUpdateException, SpaceNotAvailableException {
     PageId curPageId = new PageId();
                PageId nextPageId = new PageId();
                PageId headerPageId = null;
                Page curPage = new Page();
                HFPage hfPage = null;
                boolean curPagePinned = false;
                try {
                        try {
                                headerPageId = global.Minibase.DiskManager.get_file_entry(name);
                        } catch (Exception e) {
                                throw new HeapFileException(e, "getRecord() Failed");
                        }
                       
                        if (headerPageId == null)
                                throw new HeapFileException(null, "The heapfile has been deleted already.");
                       
                        curPageId.copyPageId(headerPageId);
                        // Try find a existing page to update the Record
                        do {
                                // Pin current page
                                try {
                                        global.Minibase.BufferManager.pinPage(curPageId, curPage, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "getRecord() Failed");
                                }
                                curPagePinned = true;

                                // try update the record in current page
                                hfPage = new HFPage(curPage);
                                try {   /*
                                        if(curPageId.pid == rid.pageno.pid) {
                                                Tuple tuple = hfPage.getRecord(rid);
                                                return tuple;
                                        }*/
                                       
                                        nextPageId.copyPageId(hfPage.getNextPage());
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "getRecord() Failed");
                                }/* catch (InvalidSlotNumberException e) {
                                        throw e;
                                }*/

                                // Unpin current page and go next
                                try {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                } catch (Exception e) {
                                        throw new HeapFileException(e, "getRecord() Failed");
                                }
                                curPagePinned = false;
                                curPageId.copyPageId(nextPageId);
                        } while (curPageId.pid != headerPageId.pid);
                       
                        // OK, we could not find a existing page containing this.
                        // Just return null.
                        return null;
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(curPagePinned) {
                                        global.Minibase.BufferManager.unpinPage(curPageId, false);
                                }
                        } catch (Exception e) {
                                throw new HeapFileException(e, "getRecord() Failed and page not unpinned");
                        }
                }

  }

        



  private void linkHFPage(PageId headerPageId, PageId newPageId, HFPage newPage)
                        throws ChainException {
                // pin both the header page and the last page in the page list (may be the same)
                boolean headerPinned = false;
                boolean tailPinned = false;
                boolean headerDirty = false;
                boolean tailDirty = false;
                PageId tailPageId = new PageId();
                Page headerPage = new Page();
                Page tailPage = new Page();
                try {
                        try {
                                 global.Minibase.BufferManager.pinPage(headerPageId, headerPage, false);
                        } catch (Exception e) {
                      
                        }
                       
                        headerPinned = true;
                       
                        HFPage headerHFPage = new HFPage(headerPage);
                        HFPage tailHFPage = null;
                        try {
                                tailPageId.copyPageId(headerHFPage.getPrevPage());
                        } catch (Exception e) {
                               
                        }
                        if(tailPageId.pid == headerPageId.pid) {
                                tailHFPage = headerHFPage;
                        }
                        else {
                                try {
                                         global.Minibase.BufferManager.pinPage(tailPageId, tailPage, false);
                                } catch (Exception e) {
                                      
                                }
                               
                                tailPinned = true;
                                tailHFPage = new HFPage(tailPage);
                        }
                       
                        // OK, all pages needed is ready. Do the modification
                        try {
                                newPage.setNextPage(headerPageId);
                                newPage.setPrevPage(tailPageId);
                                headerHFPage.setPrevPage(newPageId);
                                headerDirty = true;
                                tailHFPage.setNextPage(newPageId);
                                tailDirty = true;
                        }
                        catch(Exception e) {
                                // rollback if anything bad happens
                                try {
                                        if(headerDirty)
                                                headerHFPage.setPrevPage(tailPageId);
                                        if(tailDirty)
                                                tailHFPage.setNextPage(headerPageId);
                                }
                                catch(Exception ex) {
                                       
                                }
                     
                        }
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(headerPinned) {
                                         global.Minibase.BufferManager.unpinPage(headerPageId, headerDirty);
                                }
                        } catch (Exception e) {
                               
                        }
                        finally {
                                if(tailPinned) {
                                        try {
                                                global.Minibase.BufferManager.unpinPage(tailPageId, tailDirty);
                                        }
                                        catch(Exception e) {
                                               
                                        }
                                }
                        }
                }
        }
       
        // The newPage must have been pinned before calling this function!
        private boolean unlinkHFPage(PageId headerPageId, PageId curPageId, HFPage curPage)
                        throws ChainException {
                // Header could not be unlinked
                if(curPageId == headerPageId)
                        return false;
               
                // pin both the page before and after the newpage
                boolean prevPinned = false;
                boolean nextPinned = false;
                boolean prevDirty = false;
                boolean nextDirty = false;
                PageId prevPageId = new PageId();
                PageId nextPageId = new PageId();
                Page prevPage = new Page();
                Page nextPage = new Page();
                try {
                        try {
                                prevPageId.copyPageId(curPage.getPrevPage());
                                nextPageId.copyPageId(curPage.getNextPage());
                        }
                        catch(Exception ex) {
                           
                        }
                       
                        try {
                                 global.Minibase.BufferManager.pinPage(prevPageId, prevPage, false);
                                prevPinned = true;
                        } catch (Exception e) {
                      
                        }

                        HFPage prevHFPage = new HFPage(prevPage);
                        HFPage nextHFPage = null;
                       
                        if(nextPageId.pid == prevPageId.pid) {
                                nextHFPage = prevHFPage;
                        }
                        else {
                                try {
                                        global.Minibase.BufferManager.pinPage(nextPageId, nextPage, false);
                                        nextPinned = true;
                                } catch (Exception e) {
                       
                                }

                                nextHFPage = new HFPage(nextPage);
                        }
                       
                        // OK, all pages needed are ready. Do the modification
                        try {
                                prevHFPage.setNextPage(nextPageId);
                                prevDirty = true;
                                nextHFPage.setPrevPage(prevPageId);
                                nextDirty = true;
                        }
                        catch(Exception e) {
                                // rollback if anything bad happens
                                try {
                                        if(prevDirty)
                                                prevHFPage.setNextPage(curPageId);
                                        if(nextDirty)
                                                nextHFPage.setPrevPage(curPageId);
                                }
                                catch(Exception ex) {

                                }

                        }
                }
                finally {
                        try {
                                // unpin the page if it has been pinned
                                if(prevPinned) {
                                         global.Minibase.BufferManager.unpinPage(prevPageId, prevDirty);
                                }
                        } catch (Exception e) {
                   
                        }
                        finally {
                                if(nextPinned) {
                                        try {
                                                global.Minibase.BufferManager.unpinPage(nextPageId, nextDirty);
                                        }
                                        catch(Exception e) {
                                       
                                        }
                                }
                        }
                }
               
                return true;
        }



 

  /**

   * Searches the directory for a data page with enough free space to store a

   * record of the given size. If no suitable page is found, this creates a new

   * data page.

   */

  protected PageId getAvailPage(int reclen) {return null;}

  public HeapScan openScan(){
      return new HeapScan(this); //using this HeapFile object
  }


}
